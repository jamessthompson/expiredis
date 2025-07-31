package main

import (
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/namsral/flag"
)

const NAME string = "expiredis"

var (
	verbose     bool
	dryRun      bool
	url         string
	password    string
	pattern     string
	limit       int
	batchSize   int
	delay       int64
	ttlSubtract int
	ttlSet      int
	deleteKeys  bool
	ttlMin      int
	logger      struct {
		debug *log.Logger
		info  *log.Logger
	}
)

func main() {
	// config environment variables should be prefixed with "EXPIREDIS_"
	fs := flag.NewFlagSetWithEnvPrefix(NAME, strings.ToUpper(NAME), flag.ExitOnError)

	fs.BoolVar(&verbose, "verbose", false, "debug logging")
	fs.BoolVar(&dryRun, "dry-run", false, "dry run, no destructive commands")
	fs.StringVar(&url, "url", "redis://", "URI of Redis server (https://www.iana.org/assignments/uri-schemes/prov/redis)")
	fs.StringVar(&password, "password", "", "Redis password (can also be set via EXPIREDIS_PASSWORD env var)")
	fs.StringVar(&pattern, "pattern", "*", "Pattern of keys to process")
	fs.IntVar(&limit, "limit", 100, "Maximum number of keys to modify (delete, set TTL, etc.)")
	fs.IntVar(&batchSize, "batch-size", 500, "Keys to fetch in each batch")
	fs.Int64Var(&delay, "delay", 0, "Delay in ms between batches")
	fs.IntVar(&ttlSet, "set-ttl", 0, "Set TTL in seconds of matched keys")
	fs.IntVar(&ttlSubtract, "subtract-ttl", 0, "Seconds to subtract from TTL of matched keys")
	fs.BoolVar(&deleteKeys, "delete", false, "Delete matched keys")
	fs.IntVar(&ttlMin, "ttl-min", 0, "Minimum TTL for a key to be processed. Use -1 to match no TTL.")
	fs.Parse(os.Args[1:])

	logger.debug = log.New(os.Stderr, "[debug] ", log.LstdFlags)
	logger.info = log.New(os.Stderr, "[info] ", log.LstdFlags)
	if !verbose {
		logger.debug.SetOutput(io.Discard)
	}

	// Validate that at least one operation is specified
	if !deleteKeys && ttlSubtract == 0 && ttlSet == 0 {
		logger.info.Fatal("No operation specified. Please use --delete, --set-ttl, or --subtract-ttl")
	}

	// Output settings in verbose mode (before connection attempt)
	if verbose {
		logger.info.Println("=== Configuration Settings ===")
		logger.info.Printf("Redis URL: %s\n", url)
		if password != "" {
			logger.info.Println("Password: [SET]")
		} else {
			logger.info.Println("Password: [NOT SET]")
		}
		logger.info.Printf("Pattern: %s\n", pattern)
		logger.info.Printf("Limit: %d (modified keys)\n", limit)
		logger.info.Printf("Batch Size: %d\n", batchSize)
		logger.info.Printf("Delay: %dms\n", delay)
		logger.info.Printf("TTL Min: %d\n", ttlMin)
		logger.info.Printf("TTL Set: %d\n", ttlSet)
		logger.info.Printf("TTL Subtract: %d\n", ttlSubtract)
		logger.info.Printf("Delete Keys: %t\n", deleteKeys)
		logger.info.Printf("Dry Run: %t\n", dryRun)
		logger.info.Printf("Verbose: %t\n", verbose)
		logger.info.Println("==============================")
	}

	var c redis.Conn
	var err error

	if password != "" {
		// Use password authentication
		c, err = redis.DialURL(url, redis.DialPassword(password))
	} else {
		// Use URL-based connection (password can be in URL)
		c, err = redis.DialURL(url)
	}

	if err != nil {
		logger.info.Fatal("Failed to connect to redis: ", err)
	}
	defer c.Close()

	logger.info.Println("Connected to redis server at", url)

	if dryRun {
		logger.info.Println("Dry-run mode: destructive commands skipped")
	}

	done := make(chan func())
	scan_stats := make(chan int, 0)
	keys_stats := make(chan int, 0)
	expired_stats := make(chan int, 0)
	go stats(done, scan_stats, keys_stats, expired_stats)

	var scan struct {
		cursor   int
		batch    []string
		scanned  int
		modified int
		complete bool
	}

	scan.cursor = 0
	scan.scanned = 0
	scan.modified = 0
	scan.complete = false

	for {
		if verbose {
			logger.info.Printf("Scanning with cursor %d, pattern '%s', batch-size %d\n", scan.cursor, pattern, batchSize)
		}

		result, err := redis.Values(c.Do("SCAN", scan.cursor, "MATCH", pattern, "COUNT", batchSize))
		if err != nil {
			logger.info.Println("Failed to execute SCAN:", err)
			time.Sleep(time.Second)
			continue
		}

		_, err = redis.Scan(result, &scan.cursor, &scan.batch)
		if err != nil {
			logger.info.Println("Failed to parse response:", err)
			time.Sleep(time.Second)
			continue
		}

		if verbose {
			logger.info.Printf("Found %d keys in this batch\n", len(scan.batch))
		}

		// Separate keys that need TTL checking from those that don't
		var keysNeedingTTL []string
		var keysNoTTL []string

		for _, key := range scan.batch {
			scan.scanned++

			// Check if this key needs TTL verification
			if ttlMin != 0 || ttlSubtract != 0 {
				keysNeedingTTL = append(keysNeedingTTL, key)
			} else {
				keysNoTTL = append(keysNoTTL, key)
			}
		}

		// Process keys that don't need TTL checking immediately
		for _, key := range keysNoTTL {
			if processKeyNoTTL(c, key) {
				scan.modified++
				expired_stats <- 1

				if limit >= 0 && scan.modified >= limit {
					logger.info.Println("Reached limit of", limit, "modified keys")
					scan.complete = true
					break
				}
			}
		}

		// Process keys that need TTL checking in batches
		if len(keysNeedingTTL) > 0 && !scan.complete {
			results := processKeysBatchTTL(c, keysNeedingTTL)
			for i, shouldProcess := range results {
				if shouldProcess {
					if processKeyWithTTL(c, keysNeedingTTL[i]) {
						scan.modified++
						expired_stats <- 1

						if limit >= 0 && scan.modified >= limit {
							logger.info.Println("Reached limit of", limit, "modified keys")
							scan.complete = true
							break
						}
					}
				}
			}
		}

		// Break immediately if we've reached the limit
		if scan.complete {
			if verbose {
				logger.info.Printf("Breaking main loop: limit reached, complete=%t\n", scan.complete)
			}
			break
		}

		// Only send stats if we haven't reached the limit
		scan_stats <- 1
		keys_stats <- len(scan.batch)

		if scan.cursor == 0 {
			if verbose {
				logger.info.Printf("Breaking main loop: scan complete, cursor=%d\n", scan.cursor)
			}
			break
		}
		logger.debug.Println("Next cursor is", scan.cursor)

		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
	}

	// Read a callback from stats and call it to print final results
	(<-done)()
}

// processKeysBatchTTL processes a batch of keys that need TTL checking using pipelining
func processKeysBatchTTL(c redis.Conn, keys []string) []bool {
	results := make([]bool, len(keys))

	if len(keys) == 0 {
		return results
	}

	// Always log the keys being processed in verbose mode
	if verbose {
		for _, key := range keys {
			logger.info.Println("Processing key (batch TTL):", key)
		}
	}

	// Send all TTL commands in a pipeline
	for _, key := range keys {
		c.Send("TTL", key)
	}

	// Flush and get all results
	c.Flush()

	for i := range keys {
		ttl, err := redis.Int(c.Receive())
		if err != nil {
			logger.debug.Println("Failed to get TTL for key", keys[i])
			continue
		}

		logger.debug.Println("TTL of", ttl, "for key", keys[i])

		// Check if TTL matches criteria
		if matchTTL(ttl, ttlMin) {
			results[i] = true
		} else {
			logger.debug.Println("TTL", ttl, "doesn't match minimum TTL", ttlMin, "for key", keys[i])
		}
	}

	return results
}

// processKeyNoTTL processes a key without TTL checking (for operations that don't need TTL)
func processKeyNoTTL(c redis.Conn, key string) (expired bool) {
	// Always log the key being processed in verbose mode
	if verbose {
		logger.info.Println("Processing key (no TTL):", key)
	}

	// No TTL checking needed, so all keys match
	if !matchTTL(0, ttlMin) {
		return false
	}

	if deleteKeys {
		if dryRun {
			return true
		}

		_, err := c.Do("DEL", key)
		if err != nil {
			logger.info.Println("Failed to DELETE key", key, err)
			return false
		}

		logger.debug.Println("Deleted key", key)
		return true
	}

	if ttlSet > 0 {
		if dryRun {
			return true
		}
		_, err := c.Do("EXPIRE", key, ttlSet)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return false
		}
		logger.debug.Println("new TTL of", ttlSet, "for key", key)
		return true
	}

	return false
}

// processKeyWithTTL processes a key that has already had its TTL checked
func processKeyWithTTL(c redis.Conn, key string) (expired bool) {
	// Always log the key being processed in verbose mode
	if verbose {
		logger.info.Println("Processing key (with TTL):", key)
	}

	if deleteKeys {
		if dryRun {
			return true
		}

		_, err := c.Do("DEL", key)
		if err != nil {
			logger.info.Println("Failed to DELETE key", key, err)
			return false
		}

		logger.debug.Println("Deleted key", key)
		return true
	}

	if ttlSubtract > 0 {
		if dryRun {
			return true
		}
		// Note: We need to get TTL again since we only checked it in batch
		result, err := redis.Int(c.Do("TTL", key))
		if err != nil {
			logger.info.Println("Failed to get TTL for key", key, err)
			return false
		}
		newTTL := result - ttlSubtract
		_, err = c.Do("EXPIRE", key, newTTL)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return false
		}
		logger.debug.Println("new TTL of", newTTL, "for key", key)
		return true
	}

	if ttlSet > 0 {
		if dryRun {
			return true
		}
		_, err := c.Do("EXPIRE", key, ttlSet)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return false
		}
		logger.debug.Println("new TTL of", ttlSet, "for key", key)
		return true
	}

	return false
}

// processKey is the original function, kept for backward compatibility
func processKey(c redis.Conn, key string) (expired bool) {
	var ttl int

	// Always log the key being processed in verbose mode
	if verbose {
		logger.info.Println("Processing key:", key)
	}

	// Only fetch TTL if we need it for minimum threshold or subtracting
	if ttlMin != 0 || ttlSubtract != 0 {
		result, err := redis.Int(c.Do("TTL", key))
		if err != nil {
			logger.info.Println("Failed to get TTL for key", key)
			return
		}
		ttl = result

		logger.debug.Println("TTL of", ttl, "for key", key)
	}

	if !matchTTL(ttl, ttlMin) {
		logger.debug.Println("TTL", ttl, "doesn't match minimum TTL", ttlMin)
		return
	}

	if deleteKeys {
		if dryRun {
			return true
		}

		_, err := c.Do("DEL", key)
		if err != nil {
			logger.info.Println("Failed to DELETE key", key, err)
			return
		}

		logger.debug.Println("Deleted key", key)
		return true

	}

	if ttlSubtract > 0 {
		if dryRun {
			return true
		}
		newTTL := ttl - ttlSubtract
		_, err := c.Do("EXPIRE", key, newTTL)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return
		}
		logger.debug.Println("new TTL of", newTTL, "for key", key)
		return true
	}

	if ttlSet > 0 {
		if dryRun {
			return true
		}
		_, err := c.Do("EXPIRE", key, ttlSet)
		if err != nil {
			logger.info.Println("Failed to EXPIRE key", key, err)
			return
		}
		logger.debug.Println("new TTL of", ttlSet, "for key", key)
		return true
	}

	return
}

func matchTTL(ttl int, ttlMin int) bool {
	// No minimum TTL
	if ttlMin == 0 {
		return true
	}
	// Minimum TTL of a positive integer
	if ttlMin > 0 && ttl > ttlMin {
		return true
	}
	// No TTL, key won't expire
	if ttlMin == -1 && ttl == -1 {
		return true
	}
	return false
}

func stats(done chan func(), scans chan int, keys chan int, expired chan int) {
	timer := time.Tick(1 * time.Second)
	scan_count := 0
	keys_count := 0
	expired_count := 0

	printStats := func() {
		if dryRun {
			logger.info.Printf("Stats: scans=%d keys_scanned=%d keys_would_be_modified=%d (DRY RUN)\n",
				scan_count, keys_count, expired_count)
		} else {
			logger.info.Printf("Stats: scans=%d keys_scanned=%d keys_modified=%d\n",
				scan_count, keys_count, expired_count)
		}
	}

	for {
		select {
		case n := <-scans:
			scan_count += n

		case n := <-keys:
			keys_count += n

		case n := <-expired:
			expired_count += n

		case <-timer:
			printStats()

		case done <- printStats:
			return // Exit the goroutine after sending the final stats
		}
	}
}
