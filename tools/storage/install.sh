#!/usr/bin/env bash
# Install the preparation stage only. No producer activation, collector switch,
# bootstrap, copy, or historical import is performed by this installer.
set -euo pipefail
project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ "$project_dir" != "$HOME/crypto-market-info" || $(id -u) != 1000 ]]; then
  echo 'The checked-in units require ~/crypto-market-info and uid 1000.' >&2
  exit 1
fi
cd -- "$project_dir"
credential_dir="$HOME/.config/crypto-market-info-storage"
install -d -m 0700 "$credential_dir" var/storage/mysql/data var/storage/mysql/run var/storage/redis/data var/storage/redis/run var/storage/spool var/storage/bin
python3 - "$credential_dir/mysql-root-password" <<'PY'
import os, pathlib, secrets, sys
p = pathlib.Path(sys.argv[1])
if not p.exists():
    fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        f.write(secrets.token_hex(32)); f.flush(); os.fsync(f.fileno())
if p.stat().st_mode & 0o077:
    raise SystemExit('Existing credential file is not private.')
PY
docker pull mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242
docker pull redis@sha256:858f009f9709ce576febc734aa78b8f6d624b82571f9ddb6bda4377c833b3499
GOMAXPROCS=2 go build -o var/storage/bin/ ./cmd/...
install -d -m 0755 "$HOME/.config/systemd/user"
for component in mysql redis storage-writer; do
  install -m 0644 "deploy/systemd/crypto-market-info-$component.service" "$HOME/.config/systemd/user/crypto-market-info-$component.service"
done
systemctl --user daemon-reload
systemctl --user enable --now crypto-market-info-mysql.service crypto-market-info-redis.service
socket="$project_dir/var/storage/mysql/run/mysql.sock"
ready=false
for ((attempt=0; attempt<120; attempt++)); do
  if [[ -S "$socket" && -S "$project_dir/var/storage/redis/run/redis.sock" ]]; then ready=true; break; fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo 'Database sockets not ready; inspect the new user units.' >&2; exit 1; fi
admin=(--socket "$socket" --user root --password-file "$credential_dir/mysql-root-password")
var/storage/bin/storage-migrate init-all-schemas "${admin[@]}"
for entry in 'cmi_live live' 'cmi_query query' 'cmi_migrate migration' 'cmi_ddl ddl'; do
  read -r account role <<< "$entry"
  var/storage/bin/storage-migrate provision-account "${admin[@]}" --account "$account" --role "$role" --account-password-file "$credential_dir/$account-password"
done
var/storage/bin/storage-check --all --socket "$socket" --user cmi_query --password-file "$credential_dir/cmi_query-password" --redis-socket "$project_dir/var/storage/redis/run/redis.sock" --spool "$project_dir/var/storage/spool"
echo 'Preparation installed. The writer remains disabled; existing collectors remain unchanged.'
