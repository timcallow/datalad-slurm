#!/usr/bin/env bash
set -euo pipefail
install -d -o munge -g munge -m 0755 /run/munge
pip install -e /opt/src/datalad-slurm


# If /etc/munge is writable, chown it to munge. If it's a read-only/shared mount,
# add the munge user to the group owning the mount so it can read group-readable files.
if [ -w /etc/munge ] 2>/dev/null; then
  chown -R munge:munge /etc/munge /run/munge /var/lib/munge 2>/dev/null || true
else
  if [ -d /etc/munge ]; then
    gid=$(stat -c '%g' /etc/munge)
    # find existing group name for that gid, or create one
    grp=$(getent group "$gid" | cut -d: -f1 || true)
    if [ -z "$grp" ]; then
      grp="shared_munge_${gid}"
      groupadd -g "$gid" "$grp" 2>/dev/null || true
    fi
    # add munge to that group so it can read group-readable key files
    usermod -a -G "$grp" munge || true

    # warn if key is not group-readable — fix on host
    if [ -f /etc/munge/munge.key ]; then
      if [ ! -r /etc/munge/munge.key ] || [ "$(stat -c '%a' /etc/munge/munge.key)" -lt 440 ]; then
        echo "WARNING: /etc/munge/munge.key is not group-readable. On the host run: chmod 0440 /path/to/munge.key" >&2
      fi
    else
      echo "WARNING: /etc/munge/munge.key missing in mount; container cannot create it on a read-only mount." >&2
    fi
  fi
fi

# Start MUNGE as 'munge' via gosu, prefer syslog to avoid touching log dir
exec gosu munge /usr/sbin/munged --verbose --syslog &
sleep 1

# Optional sanity: see the cluster
sinfo || true

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

FAILED=""

for t in /opt/tests/*.sh; do
  name=$(basename "$t")
  echo
  echo "=== Running test: $name ==="

  if sh "$t"; then
    echo -e "${GREEN}✔ PASSED: $name${NC}"
  else
    echo -e "${RED}✘ FAILED: $name${NC}"
    FAILED="$FAILED $name"
  fi
done

echo

if [ -n "$FAILED" ]; then
  echo -e "${RED}Failed tests:${NC}"
  for f in $FAILED; do
    echo -e " - ${RED}$f${NC}"
  done
  exit 1
fi

echo -e "${GREEN}All light integration tests passed!"


FAILED=""
# sleep infinity & wait
SKIP_TESTS="5 8 9 14"


cat > /opt/heavy-tests/slurm_config.txt <<'EOF'
account=casus
partition=normal
USER=$(whoami)
EOF
# Need to skip tests #5, 8, 9, 14
for t in /opt/heavy-tests/test_*.sh; do
  name=$(basename "$t")

  # Extract test number (test_5_something.sh → 5)
  test_num=$(echo "$name" | sed -n 's/^test_\([0-9]\+\)_.*/\1/p' | sed 's/^0*//')


  # Skip selected tests
  for skip in $SKIP_TESTS; do
    if [ "$test_num" = "$skip" ]; then
      echo "=== Skipping test: $name ==="
      continue 2
    fi
  done

  echo
  echo "=== Running test: $name ==="
  mkdir -p "/data/heavy-tests/$name"

  args=("/data/heavy-tests/$name")
  if [ "$name" = "test_13_alt_dir.sh" ]; then
    args=("/data/heavy-tests/$name" "/data/heavy-tests/${name}_alt")
  fi

  if "$t" "${args[@]}"; then
    echo -e "${GREEN}✔ PASSED: $name${NC}"
  else
    echo -e "${RED}✘ FAILED: $name${NC}"
    FAILED="$FAILED $name"
  fi
done

echo
if [ -n "$FAILED" ]; then
  echo -e "${RED}Failed tests:${NC}"
  for f in $FAILED; do
    echo -e " - ${RED}$f${NC}"
  done
  exit 1
fi

echo -e "${GREEN}All heavy integration tests passed!"