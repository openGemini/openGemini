# Log Map

## Startup entrypoints

- `scripts/install.sh`: standalone `ts-server`
- `scripts/install_cluster.sh`: local pseudo-cluster

## Pseudo-cluster pid files

- `/tmp/openGemini/pid/meta1.pid`
- `/tmp/openGemini/pid/meta2.pid`
- `/tmp/openGemini/pid/meta3.pid`
- `/tmp/openGemini/pid/store1.pid`
- `/tmp/openGemini/pid/store2.pid`
- `/tmp/openGemini/pid/store3.pid`
- `/tmp/openGemini/pid/sql1.pid`

## Pseudo-cluster logs

- `/tmp/openGemini/logs/1/meta_extra1.log`
- `/tmp/openGemini/logs/1/store_extra1.log`
- `/tmp/openGemini/logs/1/sql_extra1.log`
- matching role logs under `/tmp/openGemini/logs/2` and `/tmp/openGemini/logs/3`

## Next code areas by symptom

- raft, member, migrate, balance: `app/ts-meta/meta/`
- shard mapping, write routing, coordinator flow: `coordinator/`
- ts-store startup or shard access: `app/ts-store/run/`, `app/ts-store/transport/`, `engine/`
- ts-sql HTTP or query routing: `app/ts-sql/sql/`
- subscriber toggles and config-sensitive startup: `config/openGemini.conf`, `Makefile`, `scripts/ut_test.sh`
