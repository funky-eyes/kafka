#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURE="${ROOT}/tools/shared_storage_minio_fixture.sh"
TMP="$(mktemp -d)"
STATE_DIR="${TMP}/state"
CACHE_DIR="${TMP}/cache"

owned_pid=""
foreign_pid=""
transition_pid=""

cleanup() {
  if [ -n "${owned_pid}" ]; then
    kill "${owned_pid}" 2>/dev/null || true
    wait "${owned_pid}" 2>/dev/null || true
  fi
  if [ -n "${foreign_pid}" ]; then
    kill "${foreign_pid}" 2>/dev/null || true
    wait "${foreign_pid}" 2>/dev/null || true
  fi
  if [ -n "${transition_pid}" ]; then
    kill "${transition_pid}" 2>/dev/null || true
    wait "${transition_pid}" 2>/dev/null || true
  fi
  rm -rf "${TMP}"
}
trap cleanup EXIT

export SHARED_STORAGE_MINIO_STATE_DIR="${STATE_DIR}"
export SHARED_STORAGE_MINIO_CACHE_DIR="${CACHE_DIR}"
# Source the fixture without running its main command so pure validation helpers
# can be exercised without downloading MinIO.
# shellcheck source=shared_storage_minio_fixture.sh
source "${FIXTURE}"

valid_pid "2"
for invalid_pid in "" "0" "1" "-1" "abc" "12x"; do
  if valid_pid "${invalid_pid}"; then
    echo "PID validation accepted invalid value: '${invalid_pid}'" >&2
    exit 1
  fi
done

case "$(uname -m)" in
  x86_64|amd64)
    platform="linux-amd64"
    ;;
  aarch64|arm64)
    platform="linux-arm64"
    ;;
  *)
    echo "Skipping MinIO PID ownership test on unsupported architecture: $(uname -m)"
    exit 0
    ;;
esac

minio_bin="${CACHE_DIR}/${platform}/minio.${platform}.RELEASE.2025-07-23T15-54-02Z"
pid_file="${STATE_DIR}/minio.pid"
mkdir -p "$(dirname "${minio_bin}")" "${STATE_DIR}"

cache_probe="${TMP}/cache-probe"
cp /bin/sleep "${cache_probe}"
chmod 0755 "${cache_probe}"
cache_probe_sha="$(sha256sum "${cache_probe}" | awk '{print $1}')"

binary_valid "${cache_probe}" "sleep (GNU coreutils)" "${cache_probe_sha}"

fake_bin="${TMP}/fake-bin"
mkdir -p "${fake_bin}"
cat > "${fake_bin}/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then
    output="$2"
    shift 2
    continue
  fi
  shift
done
[ -n "${output}" ] || exit 2
printf 'partial-download' > "${output}"
exit 22
EOF
chmod 0755 "${fake_bin}/curl"

download_target="${TMP}/failed-download"
if PATH="${fake_bin}:${PATH}" download_verified     "https://example.invalid/minio" "${download_target}" "fake-release"     "0000000000000000000000000000000000000000000000000000000000000000"; then
  echo "Failed MinIO fixture download unexpectedly succeeded" >&2
  exit 1
fi
if compgen -G "${download_target}.download.*" >/dev/null; then
  echo "Failed MinIO fixture download left a partial file behind" >&2
  exit 1
fi

if binary_valid "${cache_probe}" "sleep (GNU coreutils)"     "0000000000000000000000000000000000000000000000000000000000000000"; then
  echo "Cache validation accepted an incorrect SHA-256" >&2
  exit 1
fi
if binary_valid "${cache_probe}" "not-a-real-release" "${cache_probe_sha}"; then
  echo "Cache validation accepted an incorrect release string" >&2
  exit 1
fi

# Use a copied ELF executable so /proc/<pid>/exe resolves to the exact
# path the fixture considers its MinIO binary without requiring network access.
cp /bin/sleep "${minio_bin}"
chmod 0755 "${minio_bin}"

bash -c 'sleep 0.2; exec "$1" 60' _ "${minio_bin}" &
transition_pid="$!"
wait_for_fixture_exec "${transition_pid}"
if [ "$(process_executable "${transition_pid}")" != "$(fixture_executable)" ]; then
  echo "Fixture exec wait returned before process became the pinned binary" >&2
  exit 1
fi
kill "${transition_pid}" 2>/dev/null || true
wait "${transition_pid}" 2>/dev/null || true
transition_pid=""

fixture() {
  env \
    SHARED_STORAGE_MINIO_STATE_DIR="${STATE_DIR}" \
    SHARED_STORAGE_MINIO_CACHE_DIR="${CACHE_DIR}" \
    "${FIXTURE}" "$@"
}

"${minio_bin}" 60 &
owned_pid="$!"
owned_start="$(process_start_time "${owned_pid}")"
write_pid_identity "${owned_pid}"
if compgen -G "${pid_file}.tmp.*" >/dev/null; then
  echo "PID identity publication left a temporary file behind" >&2
  exit 1
fi

fixture status | grep -Fq "running pid=${owned_pid}"

# A PID without the process start-time token is legacy/stale state and must not
# be trusted even when it points at the expected executable.
printf '%s\n' "${owned_pid}" > "${pid_file}"
if fixture status >/dev/null 2>&1; then
  echo "Fixture status accepted PID state without a start-time token" >&2
  exit 1
fi
missing_token_err="${TMP}/missing-token-stop.err"
fixture stop 2>"${missing_token_err}"
if ! kill -0 "${owned_pid}" 2>/dev/null; then
  echo "Fixture stop killed process whose PID state lacked a start-time token" >&2
  exit 1
fi
grep -Fq "Refusing to stop stale MinIO PID ${owned_pid}" "${missing_token_err}"

write_pid_identity "${owned_pid}"

# The same executable path and PID are not enough: a mismatched start-time token
# simulates a reused PID and must fail closed without killing the live process.
printf '%s %s\n' "${owned_pid}" "$((owned_start + 1))" > "${pid_file}"
if fixture status >/dev/null 2>&1; then
  echo "Fixture status accepted a mismatched process start-time token" >&2
  exit 1
fi
stale_token_err="${TMP}/stale-token-stop.err"
fixture stop 2>"${stale_token_err}"
if ! kill -0 "${owned_pid}" 2>/dev/null; then
  echo "Fixture stop killed process with mismatched start-time token" >&2
  exit 1
fi
grep -Fq "Refusing to stop stale MinIO PID ${owned_pid}" "${stale_token_err}"

write_pid_identity "${owned_pid}"
fixture stop
if kill -0 "${owned_pid}" 2>/dev/null; then
  echo "Fixture stop did not terminate its owned process ${owned_pid}" >&2
  exit 1
fi
wait "${owned_pid}" 2>/dev/null || true
owned_pid=""

sleep 60 &
foreign_pid="$!"
echo "${foreign_pid}" > "${pid_file}"

stderr_file="${TMP}/stale-stop.err"
fixture stop 2>"${stderr_file}"
if ! kill -0 "${foreign_pid}" 2>/dev/null; then
  echo "Fixture stop killed unrelated process ${foreign_pid}" >&2
  exit 1
fi
if [ -e "${pid_file}" ]; then
  echo "Fixture stop did not clear stale PID file" >&2
  exit 1
fi
grep -Fq "Refusing to stop stale MinIO PID ${foreign_pid}" "${stderr_file}"

echo "MinIO fixture integrity and PID ownership tests passed"
