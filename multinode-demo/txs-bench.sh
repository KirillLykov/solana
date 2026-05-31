#!/usr/bin/env bash
set -e

here=$(dirname "$0")
# shellcheck source=multinode-demo/common.sh
source "$here"/common.sh

program=$solana_transaction_bench

usage() {
  if [[ -n $1 ]]; then
    echo "$*"
    echo
  fi
  cat <<EOF
usage: $0 [extra args]

 Run solana-transaction-bench against the local multinode demo.

   extra args: additional arguments are passed along to solana-transaction-bench.
               Use solana-transaction-bench argument names, not bench-tps names.

 Defaults:
   --url http://127.0.0.1:8899
   --authority ${SOLANA_CONFIG_DIR}/faucet.json
   run
   --num-payers 256
   --payer-account-balance 1SOL
   --duration 90
   --bind 127.0.0.1:0
   --staked-identity-file ${SOLANA_CONFIG_DIR}/bootstrap-validator/identity.json
   --transfer-tx-cu-budget 600
   pinned-leader-tracker 127.0.0.1:8002

 Examples:
   $0 --duration 30 --target-tps 5000
   $0 --num-payers 1024 --send-fanout 2 ws-leader-tracker

EOF
  exit 1
}

contains_arg() {
  declare name=$1
  shift

  for arg in "$@"; do
    if [[ $arg = "$name" || $arg = "$name="* ]]; then
      return 0
    fi
  done

  return 1
}

contains_url_arg() {
  for arg in "$@"; do
    if [[ $arg = --url || $arg = --url=* || $arg = -u || $arg = -u?* ]]; then
      return 0
    fi
  done

  return 1
}

add_default_arg() {
  declare -n target_args=$1
  declare name=$2
  declare value=$3

  if ! contains_arg "$name" "${target_args[@]}"; then
    target_args+=("$name" "$value")
  fi
}

global_args=()
run_args=()
leader_tracker_args=()

while [[ -n $1 ]]; do
  case "$1" in
  -h | --help)
    usage
    ;;
  --url | -u | --commitment-config | --authority)
    [[ -n ${2:-} ]] || usage "Missing value for $1"
    global_args+=("$1" "$2")
    shift 2
    ;;
  --url=* | --commitment-config=* | --authority=* | -u?*)
    global_args+=("$1")
    shift
    ;;
  --validate-accounts | --mock-rpc)
    global_args+=("$1")
    shift
    ;;
  run | read-accounts-run | write-accounts)
    usage "Do not pass a solana-transaction-bench subcommand; this wrapper always uses run."
    ;;
  pinned-leader-tracker | legacy-leader-tracker | ws-leader-tracker | \
    yellowstone-leader-tracker | custom-leader-tracker)
    leader_tracker_args=("$@")
    break
    ;;
  *)
    run_args+=("$1")
    shift
    ;;
  esac
done

if ! command -v "$program" > /dev/null 2>&1; then
  echo "$program not found."
  echo "Install it with: cargo install solana-transaction-bench"
  echo "Or set SOLANA_TRANSACTION_BENCH=/path/to/solana-transaction-bench."
  exit 1
fi

if ! contains_url_arg "${global_args[@]}"; then
  global_args+=(--url "http://127.0.0.1:8899")
fi
add_default_arg global_args --authority "${SOLANA_CONFIG_DIR}/faucet.json"

add_default_arg run_args --num-payers 256
add_default_arg run_args --payer-account-balance 1SOL
add_default_arg run_args --duration 90
add_default_arg run_args --bind "127.0.0.1:0"
add_default_arg run_args --staked-identity-file "${SOLANA_CONFIG_DIR}/bootstrap-validator/identity.json"
add_default_arg run_args --transfer-tx-cu-budget 600

if [[ ${#leader_tracker_args[@]} -eq 0 ]]; then
  leader_tracker_args=(pinned-leader-tracker "127.0.0.1:8002")
fi

"$program" "${global_args[@]}" run "${run_args[@]}" "${leader_tracker_args[@]}"
