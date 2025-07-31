**Warning:** *this is alpha software. If you're going to use it in production, please read the source code first.*

## Usage

```
Usage of ./expiredis:
  --batch-size=100: Keys to fetch in each batch
  --delay=0: Delay in ms between batches
  --delete=false: Delete matched keys
  --dry-run=false: dry run, no destructive commands
  --limit=100: Maximum number of keys to modify (delete, set TTL, etc.)
  --password="": Redis password (can also be set via EXPIREDIS_PASSWORD env var)
  --pattern="*": Pattern of keys to process
  --set-ttl=0: Set TTL in seconds of matched keys
  --subtract-ttl=0: Seconds to subtract from TTL of matched keys
  --ttl-min=0: Minimum TTL for a key to be processed. Use -1 to match no TTL.
  --url="redis://": URI of Redis server (https://www.iana.org/assignments/uri-schemes/prov/redis)
  --verbose=false: debug logging
```

## Examples

### Set a TTL of 3600 (1 hour) for all keys without a TTL, processing 1000 keys at a time

```bash
./expiredis --limit -1 --batch-size 1000 --set-ttl 3600
```

### Delete all keys matching `foo:*`

```bash
./expiredis --limit -1 --pattern "foo:*" --delete
```

### Reduce expiry by 86400 seconds of keys matching `bar:*` with a minimum TTL of 86400 (1 day)

```bash
./expiredis --limit -1 --pattern "bar:*" --subtract-ttl 86400 --ttl-min 86400
```

### Limit the number of keys modified (useful for safe testing)

```bash
# Only modify the first 10 keys that match the criteria
./expiredis --limit 10 --pattern "user:*" --set-ttl 3600
```

### Only process keys that have no TTL set (use --ttl-min -1)

```bash
./expiredis --ttl-min -1 --set-ttl 3600 --pattern "user:*"
```

### Use password authentication

```bash
# Command line password
./expiredis --password "mysecret" --url "redis://localhost:6379"

# Environment variable password
EXPIREDIS_PASSWORD="mysecret" ./expiredis --url "redis://localhost:6379"

# Password in URL (still supported)
./expiredis --url "redis://:mysecret@localhost:6379"
```

### Dry run mode (preview changes without making them)

```bash
./expiredis --dry-run --pattern "temp:*" --delete
```

## Environment Variables

All flags can also be set via environment variables with the `EXPIREDIS_` prefix:

```bash
export EXPIREDIS_PASSWORD="mysecret"
export EXPIREDIS_PATTERN="user:*"
export EXPIREDIS_DRY_RUN="true"
./expiredis
```

## Building

### Build for current platform
```bash
go build
```
