#!/usr/bin/env bash
set -euo pipefail

readonly example_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly image_name="docker.io/openbao/openbao:2.6.1"
readonly container_name="valkyr-openbao-dev"
readonly root_token="dev-only-token"
readonly secret_id_file="${example_dir}/openbao-secret-id"
readonly adapter_config_template="${example_dir}/openbao-security-config.yml"
readonly localhost_adapter_config="${example_dir}/openbao-security-config-localhost.yml"
readonly docker_adapter_config="${example_dir}/openbao-security-config-host.docker.internal.yml"
runtime=""

select_runtime() {
  case "${OPENBAO_RUNTIME:-auto}" in
    auto)
      if command -v docker >/dev/null 2>&1; then
        runtime="docker"
      elif command -v podman >/dev/null 2>&1; then
        runtime="podman"
      elif command -v container >/dev/null 2>&1; then
        runtime="container"
      fi
      ;;
    docker|podman|container)
      runtime="$OPENBAO_RUNTIME"
      ;;
    *)
      echo "OPENBAO_RUNTIME must be 'docker', 'podman', or 'container'." >&2
      exit 1
      ;;
  esac

  command -v "$runtime" >/dev/null 2>&1 || {
    echo "Install Docker, Podman, or Apple's container CLI to bootstrap local OpenBao." >&2
    exit 1
  }
}

start_openbao() {
  if container_exists; then
    if ! container_is_running; then
      echo "Container ${container_name} exists but is not running." >&2
      exit 1
    fi
    return
  fi

  if [[ "$runtime" == "container" ]]; then
    container system start
  fi
  "$runtime" run --detach --rm --name "$container_name" \
    --publish 127.0.0.1:8200:8200 "$image_name" \
    server -dev \
    -dev-listen-address=0.0.0.0:8200 \
    -dev-root-token-id="$root_token" >/dev/null
}

container_exists() {
  if [[ "$runtime" == "docker" || "$runtime" == "podman" ]]; then
    "$runtime" container inspect "$container_name" >/dev/null 2>&1
  else
    container inspect "$container_name" >/dev/null 2>&1
  fi
}

container_is_running() {
  if [[ "$runtime" == "docker" || "$runtime" == "podman" ]]; then
    [[ "$("$runtime" inspect -f '{{.State.Running}}' "$container_name")" == "true" ]]
  else
    container inspect "$container_name" | grep -Eq '"state"[[:space:]]*:[[:space:]]*"running"'
  fi
}

run_bao() {
  "$runtime" exec \
    "$container_name" \
    env \
    BAO_ADDR=http://127.0.0.1:8200 \
    BAO_TOKEN="$root_token" \
    bao "$@"
}

wait_for_openbao() {
  for _ in {1..30}; do
    if run_bao status >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done

  echo "OpenBao did not become ready within 30 seconds." >&2
  exit 1
}

configure_openbao() {
  if ! run_bao secrets list -format=json | grep -q '"valkyr/"'; then
    run_bao secrets enable -path=valkyr kv-v2
  fi
  if ! run_bao auth list -format=json | grep -q '"approle/"'; then
    run_bao auth enable approle
  fi
  "$runtime" exec --interactive \
    "$container_name" \
    env \
    BAO_ADDR=http://127.0.0.1:8200 \
    BAO_TOKEN="$root_token" \
    bao policy write valkyr-security - <<'POLICY'
path "valkyr/metadata/security/*" {
  capabilities = ["read", "list"]
}

path "valkyr/data/security/*" {
  capabilities = ["create", "read", "list", "update", "delete"]
}
POLICY
  run_bao write auth/approle/role/valkyr-security \
    token_policies=valkyr-security \
    token_ttl=1h \
    token_max_ttl=4h \
    secret_id_ttl=24h
}

write_adapter_credentials() {
  local role_id secret_id
  role_id="$(run_bao read -field=role_id auth/approle/role/valkyr-security/role-id)"
  secret_id="$(run_bao write -field=secret_id -f auth/approle/role/valkyr-security/secret-id)"

  umask 077
  render_adapter_config() {
    local endpoint_url="$1"
    local ca_certificate_file="$2"
    local output_file="$3"

    sed \
      -e "s#replace-with-openbao-role-id#${role_id}#" \
      -e "s#url: tls://[^[:space:]]*#url: ${endpoint_url}#" \
      -e "s#ca_certificate_file: ./tls/[^[:space:]]*#ca_certificate_file: ${ca_certificate_file}#" \
      "${adapter_config_template}" >"${output_file}"
  }

  render_adapter_config \
    "tls://localhost:8443" \
    "./tls/localhost.crt" \
    "${localhost_adapter_config}"
  render_adapter_config \
    "tls://host.docker.internal:8443" \
    "./tls/host.docker.internal.crt" \
    "${docker_adapter_config}"

  printf '%s\n' "$secret_id" >"$secret_id_file"
}

main() {
  select_runtime
  start_openbao
  wait_for_openbao
  configure_openbao
  write_adapter_credentials

  printf 'OpenBao is ready at http://localhost:8200.\n'
  printf 'Run locally: cargo run -p valkyr-openbao-adapter -- --config %s\n' \
    "$localhost_adapter_config"
  printf 'Run for Docker host access: cargo run -p valkyr-openbao-adapter -- --config %s\n' \
    "$docker_adapter_config"
}

main "$@"
