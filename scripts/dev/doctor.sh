#!/usr/bin/env bash
set -euo pipefail

print_result() {
  local label="$1"
  local value="$2"
  printf '%-16s %s\n' "$label" "$value"
}

check_command() {
  local label="$1"
  local probe="$2"
  shift 2

  if ! command -v "$probe" >/dev/null 2>&1; then
    print_result "$label" "MISSING"
    return 0
  fi

  local output
  if output="$("$@" 2>&1 | head -n 1)"; then
    print_result "$label" "$output"
  else
    print_result "$label" "ERROR: ${output:-version check failed}"
  fi
}

required_rust_channel() {
  awk -F '"' '/^[[:space:]]*channel[[:space:]]*=/ { print $2; exit }' rust-toolchain.toml
}

check_rust_toolchain() {
  local required
  required="$(required_rust_channel)"

  if [[ -z "$required" ]]; then
    print_result "rustc" "ERROR: rust-toolchain.toml has no channel"
    print_result "cargo" "ERROR: rust-toolchain.toml has no channel"
    return 0
  fi

  if command -v rustup >/dev/null 2>&1; then
    local installed
    installed="$(rustup toolchain list 2>/dev/null || true)"

    local toolchain
    toolchain="$(printf '%s\n' "$installed" | awk -v channel="$required" '$1 == channel || index($1, channel "-") == 1 { print $1; exit }')"

    if [[ -z "$toolchain" ]]; then
      print_result "rustc" "MISSING (required ${required})"
      print_result "cargo" "MISSING (required ${required})"
      return 0
    fi

    local rustc_version cargo_version
    rustc_version="$(rustup run "$toolchain" rustc --version 2>&1 | head -n 1 || true)"
    cargo_version="$(rustup run "$toolchain" cargo --version 2>&1 | head -n 1 || true)"

    print_result "rustc" "${rustc_version:-ERROR: version check failed}"
    print_result "cargo" "${cargo_version:-ERROR: version check failed}"
    return 0
  fi

  check_command "rustc" "rustc" rustc --version
  check_command "cargo" "cargo" cargo --version
}

check_command "git" "git" git --version
check_command "make" "make" make --version
check_rust_toolchain
check_command "go" "go" env GOTOOLCHAIN=local go version
check_command "python" "python3" python3 --version
check_command "uv" "uv" uv --version
check_command "buf" "buf" buf --version
check_command "docker" "docker" docker --version

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  print_result "compose" "$(docker compose version 2>&1 | head -n 1)"
else
  print_result "compose" "MISSING"
fi
