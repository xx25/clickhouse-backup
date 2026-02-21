# Test Plan: Local Storage Backend

## Environment

- ClickHouse 25.12, single `default` disk at `/var/lib/clickhouse/`
- Database `nodelistdb` with 4 tables: `flag_statistics` (ReplacingMergeTree), `node_test_daily_stats` (SummingMergeTree), `node_test_results` (MergeTree), `nodes` (MergeTree)
- Binary: `./clickhouse-backup` built from source

## Config

Create `/tmp/clickhouse-backup-local-test.yml`:

```yaml
general:
  remote_storage: local
  backups_to_keep_local: 3
  backups_to_keep_remote: 3
clickhouse:
  username: default
  host: localhost
  port: 9000
local:
  path: /tmp/clickhouse-backup-test/remote
  compression_format: tar
  compression_level: 1
  debug: true
```

All commands below use:
```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml <command>
```

## Test Sequence

### 1. Config and connectivity

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml tables
```

**Expected:** Lists `nodelistdb` tables. Validates config loading and ClickHouse connection.

### 2. Create local backup

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml create backup1
```

**Expected:** Backup created in `/var/lib/clickhouse/backup/backup1/`. Validates FREEZE + hardlink logic.

### 3. Upload to local storage

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml upload backup1
```

**Expected:** Files appear under `/tmp/clickhouse-backup-test/remote/backup1/`. Validates `Connect`, `PutFile`, `PutFileAbsolute`, compression, and `Walk`.

**Verify:**
```bash
find /tmp/clickhouse-backup-test/remote/backup1/ -type f
```

### 4. List backups (local + remote)

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml list
```

**Expected:** Shows `backup1` as both local and remote. Validates `BackupList`, `Walk` (non-recursive), `StatFile`, `GetFileReader` (metadata.json parsing).

### 5. List remote only

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml list remote
```

**Expected:** Shows `backup1` with correct size and date.

### 6. Insert data and create second backup

```bash
clickhouse-client --query "INSERT INTO nodelistdb.nodes VALUES (now(), 'test-node-backup2', 'test', 0, 0, 0, 0, '', '')"
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml create backup2
```

**Expected:** `backup2` created locally with updated data.

### 7. Incremental upload

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml upload backup2 --diff-from backup1
```

**Expected:** Only changed/new parts uploaded. `/tmp/clickhouse-backup-test/remote/backup2/` should be smaller than `backup1` if parts were deduplicated. Validates incremental logic through the `RemoteStorage` abstraction.

**Verify:**
```bash
du -sh /tmp/clickhouse-backup-test/remote/backup1/
du -sh /tmp/clickhouse-backup-test/remote/backup2/
```

### 8. Delete local backups (force download path)

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete local backup1
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete local backup2
```

**Expected:** Local backups removed from `/var/lib/clickhouse/backup/`.

**Verify:**
```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml list local
```

### 9. Download from local storage

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml download backup2
```

**Expected:** Backup downloaded and decompressed to `/var/lib/clickhouse/backup/backup2/`. Validates `GetFileReader`, `GetFileReaderAbsolute`, `StatFile`, decompression.

**Verify:**
```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml list local
```

### 10. Restore data

```bash
clickhouse-client --query "DROP TABLE IF EXISTS nodelistdb.nodes"
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml restore --schema --data backup2
```

**Expected:** Table recreated with data including the row inserted in step 6.

**Verify:**
```bash
clickhouse-client --query "SELECT count() FROM nodelistdb.nodes"
```

### 11. Delete remote backup (batch delete)

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete remote backup1
```

**Expected:** `/tmp/clickhouse-backup-test/remote/backup1/` removed. Validates `DeleteKeysBatch` via `RemoveBackupRemote` → `BatchDeleter` path.

**Verify:**
```bash
ls /tmp/clickhouse-backup-test/remote/
```

### 12. Combined create + upload

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml create_remote backup3
```

**Expected:** Creates local backup and uploads to remote in one command. Validates end-to-end flow.

**Verify:**
```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml list
ls /tmp/clickhouse-backup-test/remote/backup3/
```

## Cleanup

```bash
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete local backup1
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete local backup2
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete local backup3
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete remote backup2
sudo ./clickhouse-backup -c /tmp/clickhouse-backup-local-test.yml delete remote backup3
rm -rf /tmp/clickhouse-backup-test /tmp/clickhouse-backup-local-test.yml
```

## Success Criteria

All 12 steps complete without errors. Key validations:

- `local.path` directory is created automatically on `Connect`
- Compressed archives appear under the configured path
- `list remote` parses `metadata.json` correctly
- Incremental uploads produce smaller remote footprint
- Download + restore round-trips data correctly
- `delete remote` cleans up all files via `BatchDeleter`
- `containedPath` prevents path traversal (no `../` escape)
