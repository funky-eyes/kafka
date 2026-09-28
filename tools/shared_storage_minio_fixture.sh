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

MINIO_RELEASE="RELEASE.2025-07-23T15-54-02Z"
MC_RELEASE="RELEASE.2025-07-21T05-28-08Z"
ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
ENDPOINT="${SHARED_STORAGE_MINIO_ENDPOINT:-http://127.0.0.1:9000}"
ADDRESS="${SHARED_STORAGE_MINIO_ADDRESS:-127.0.0.1:9000}"
CONSOLE_ADDRESS="${SHARED_STORAGE_MINIO_CONSOLE_ADDRESS:-127.0.0.1:9001}"
STATE_DIR="${SHARED_STORAGE_MINIO_STATE_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}/kafka-shared-storage-minio}"
CACHE_DIR="${SHARED_STORAGE_MINIO_CACHE_DIR:-${HOME}/.cache/kafka-shared-storage-minio}"
DATA_DIR="${STATE_DIR}/data"
PID_FILE="${STATE_DIR}/minio.pid"
LOG_FILE="${STATE_DIR}/minio.log"
MC_CONFIG_DIR="${STATE_DIR}/mc-config"

case "$(uname -m)" in
  x86_64|amd64)
    PLATFORM="linux-amd64"
    MINIO_SHA256="eef6581f6509f43ece007a6f2eb4c5e3ce41498c8956e919a7ac7b4b170fa431"
    MC_SHA256="ea4a453be116071ab1ccbd24eb8755bf0579649f41a7b94ab9e68571bb9f4a1e"
    ;;
  aarch64|arm64)
    PLATFORM="linux-arm64"
    MINIO_SHA256="ec9f48fee97dee7e6ce8fe9f576471bcd52380c035091d8b075b64d606124c7e"
    MC_SHA256="62a7d4cc0ea37e7a5b564e0991057d4fec2a563566511fabe4ee5c1568f5e00b"
    ;;
  *)
    echo "Unsupported MinIO fixture architecture: $(uname -m)" >&2
    exit 2
    ;;
esac

BIN_DIR="${CACHE_DIR}/${PLATFORM}"
MINIO_RELEASE_FILE="minio.${PLATFORM}.${MINIO_RELEASE}"
MC_RELEASE_FILE="mc.${PLATFORM}.${MC_RELEASE}"
MINIO_BIN="${BIN_DIR}/${MINIO_RELEASE_FILE}"
MC_BIN="${BIN_DIR}/${MC_RELEASE_FILE}"

server_base_url="https://github.com/minio/minio/releases/download/${MINIO_RELEASE}"
mc_base_url="https://github.com/minio/mc/releases/download/${MC_RELEASE}"

script_path="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"

mkdir -p "${STATE_DIR}" "${CACHE_DIR}"

binary_valid() {
  local path="$1"
  local expected_release="$2"
  local expected_sha256="$3"
  local actual

  [ -x "${path}" ] || return 1
  actual="$(sha256sum "${path}" | awk '{print $1}')"
  [ "${actual}" = "${expected_sha256}" ] || return 1
  "${path}" --version 2>&1 | grep -Fq "${expected_release}"
}

download_verified() {
  local url="$1"
  local destination="$2"
  local expected_release="$3"
  local expected_sha256="$4"
  local temporary actual

  if binary_valid "${destination}" "${expected_release}" "${expected_sha256}"; then
    return
  fi
  if [ -e "${destination}" ]; then
    echo "Discarding invalid cached MinIO fixture binary: ${destination}" >&2
    rm -f "${destination}"
  fi

  mkdir -p "$(dirname "${destination}")"
  temporary="${destination}.download"
  rm -f "${temporary}"

  curl --fail --location --silent --show-error \
    --retry 5 --retry-all-errors --connect-timeout 20 \
    --output "${temporary}" "${url}"

  actual="$(sha256sum "${temporary}" | awk '{print $1}')"
  if [ "${actual}" != "${expected_sha256}" ]; then
    rm -f "${temporary}"
    echo "Checksum mismatch downloading ${url}: expected=${expected_sha256} actual=${actual}" >&2
    exit 1
  fi

  chmod 0755 "${temporary}"
  mv "${temporary}" "${destination}"

  if ! "${destination}" --version 2>&1 | grep -Fq "${expected_release}"; then
    rm -f "${destination}"
    echo "Downloaded binary does not report expected release ${expected_release}: ${url}" >&2
    exit 1
  fi
}

install_fixture() {
  download_verified "${server_base_url}/${MINIO_RELEASE_FILE}" "${MINIO_BIN}" "${MINIO_RELEASE}" "${MINIO_SHA256}"
  download_verified "${mc_base_url}/${MC_RELEASE_FILE}" "${MC_BIN}" "${MC_RELEASE}" "${MC_SHA256}"
}

pid_value() {
  if [ -f "${PID_FILE}" ]; then
    cat "${PID_FILE}"
  fi
}

process_executable() {
  local pid="$1"
  readlink -f "/proc/${pid}/exe" 2>/dev/null || true
}

fixture_executable() {
  readlink -f "${MINIO_BIN}" 2>/dev/null || true
}

valid_pid() {
  local pid="$1"
  case "${pid}" in
    ''|*[!0-9]*)
      return 1
      ;;
  esac
  [ "${pid}" -gt 1 ]
}

pid_is_fixture() {
  local pid="$1"
  local actual expected
  valid_pid "${pid}" || return 1
  kill -0 -- "${pid}" 2>/dev/null || return 1
  actual="$(process_executable "${pid}")"
  expected="$(fixture_executable)"
  [ -n "${actual}" ] && [ -n "${expected}" ] && [ "${actual}" = "${expected}" ]
}

is_running() {
  local pid
  pid="$(pid_value)"
  pid_is_fixture "${pid}"
}

discard_stale_pid() {
  local pid
  pid="$(pid_value)"
  [ -n "${pid}" ] || return
  if ! pid_is_fixture "${pid}"; then
    echo "Discarding stale MinIO PID file: pid=${pid}" >&2
    rm -f "${PID_FILE}"
  fi
}

export_github_environment() {
  if [ -n "${GITHUB_ENV:-}" ]; then
    {
      echo "SHARED_STORAGE_MINIO_STATE_DIR=${STATE_DIR}"
      echo "SHARED_STORAGE_MINIO_CACHE_DIR=${CACHE_DIR}"
      echo "SHARED_STORAGE_S3_CONTROL=${script_path}"
    } >> "${GITHUB_ENV}"
  fi
}

start_server() {
  install_fixture
  export_github_environment
  mkdir -p "${DATA_DIR}" "${MC_CONFIG_DIR}"

  if is_running; then
    return
  fi
  discard_stale_pid

  {
    echo "=== starting MinIO ${MINIO_RELEASE} at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  } >> "${LOG_FILE}"

  nohup env \
    MINIO_ROOT_USER="${ROOT_USER}" \
    MINIO_ROOT_PASSWORD="${ROOT_PASSWORD}" \
    "${MINIO_BIN}" server "${DATA_DIR}" \
      --address "${ADDRESS}" \
      --console-address "${CONSOLE_ADDRESS}" \
      >> "${LOG_FILE}" 2>&1 &
  echo "$!" > "${PID_FILE}"
}

stop_server() {
  local pid
  pid="$(pid_value)"
  if [ -z "${pid}" ]; then
    return
  fi
  if ! pid_is_fixture "${pid}"; then
    echo "Refusing to stop stale MinIO PID ${pid}: process does not belong to this fixture" >&2
    rm -f "${PID_FILE}"
    return
  fi

  kill -- "${pid}" 2>/dev/null || true
  for _ in $(seq 1 50); do
    if ! pid_is_fixture "${pid}"; then
      rm -f "${PID_FILE}"
      return
    fi
    sleep 0.1
  done

  if pid_is_fixture "${pid}"; then
    kill -KILL -- "${pid}" 2>/dev/null || true
  fi
  for _ in $(seq 1 20); do
    if ! pid_is_fixture "${pid}"; then
      break
    fi
    sleep 0.1
  done
  rm -f "${PID_FILE}"
}

ready() {
  curl --fail --silent --show-error "${ENDPOINT}/minio/health/ready" >/dev/null
}

wait_ready() {
  local timeout_seconds="${1:-60}"
  local deadline=$((SECONDS + timeout_seconds))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if ready; then
      return
    fi
    if ! is_running; then
      break
    fi
    sleep 1
  done
  echo "MinIO did not become ready within ${timeout_seconds}s" >&2
  show_logs >&2 || true
  return 1
}

mc_alias() {
  install_fixture
  mkdir -p "${MC_CONFIG_DIR}"
  "${MC_BIN}" --config-dir "${MC_CONFIG_DIR}" alias set local "${ENDPOINT}" "${ROOT_USER}" "${ROOT_PASSWORD}" >/dev/null
}

make_bucket() {
  local bucket="$1"
  mc_alias
  "${MC_BIN}" --config-dir "${MC_CONFIG_DIR}" mb --ignore-existing "local/${bucket}" >/dev/null
}

list_bucket() {
  local bucket="$1"
  mc_alias
  "${MC_BIN}" --config-dir "${MC_CONFIG_DIR}" ls --recursive "local/${bucket}"
}

du_bucket() {
  local bucket="$1"
  mc_alias
  "${MC_BIN}" --config-dir "${MC_CONFIG_DIR}" du --recursive "local/${bucket}"
}

find_bucket() {
  local bucket="$1"
  mc_alias
  "${MC_BIN}" --config-dir "${MC_CONFIG_DIR}" find "local/${bucket}"
}

show_logs() {
  if [ -f "${LOG_FILE}" ]; then
    cat "${LOG_FILE}"
  else
    echo "MinIO log does not exist yet: ${LOG_FILE}"
  fi
}

main() {
  case "${1:-}" in
    install)
      install_fixture
      ;;
    start)
      start_server
      ;;
    stop)
      stop_server
      ;;
    restart)
      stop_server
      start_server
      ;;
    wait-ready)
      wait_ready "${2:-60}"
      ;;
    mb)
      [ "$#" -eq 2 ] || { echo "usage: $0 mb <bucket>" >&2; exit 2; }
      make_bucket "$2"
      ;;
    ls)
      [ "$#" -eq 2 ] || { echo "usage: $0 ls <bucket>" >&2; exit 2; }
      list_bucket "$2"
      ;;
    du)
      [ "$#" -eq 2 ] || { echo "usage: $0 du <bucket>" >&2; exit 2; }
      du_bucket "$2"
      ;;
    find)
      [ "$#" -eq 2 ] || { echo "usage: $0 find <bucket>" >&2; exit 2; }
      find_bucket "$2"
      ;;
    logs)
      show_logs
      ;;
    status)
      if is_running; then
        echo "running pid=$(pid_value)"
      else
        echo "stopped"
        return 1
      fi
      ;;
    *)
      echo "usage: $0 {install|start|stop|restart|wait-ready [seconds]|mb <bucket>|ls <bucket>|du <bucket>|find <bucket>|logs|status}" >&2
      return 2
      ;;
  esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
