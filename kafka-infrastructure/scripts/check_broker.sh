#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${INFRA_DIR}/docker-compose.yml"
COMPOSE=(docker compose --project-directory "${INFRA_DIR}" -f "${COMPOSE_FILE}")

fail() {
    echo "LỖI: $*" >&2
    "${COMPOSE[@]}" logs --tail=100 kafka >&2 || true
    exit 1
}

command -v docker >/dev/null 2>&1 || {
    echo "LỖI: Không tìm thấy Docker CLI." >&2
    exit 1
}

docker compose version >/dev/null 2>&1 || {
    echo "LỖI: Cần Docker Compose v2 (docker compose)." >&2
    exit 1
}

docker info >/dev/null 2>&1 || {
    echo "LỖI: Docker daemon chưa chạy hoặc không thể truy cập." >&2
    exit 1
}

"${COMPOSE[@]}" config --quiet || fail "Cấu hình Compose không hợp lệ. Hãy sao chép .env.example thành .env rồi cấu hình."

container_id="$("${COMPOSE[@]}" ps -q kafka)"
[[ -n "${container_id}" ]] || fail "Container Kafka chưa được tạo. Hãy chạy: docker compose up -d"

running="$(docker inspect --format '{{.State.Running}}' "${container_id}")"
[[ "${running}" == "true" ]] || fail "Container Kafka chưa chạy."

health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "${container_id}")"
if [[ "${health}" != "healthy" ]]; then
    fail "Trạng thái health của container Kafka là '${health}', phải là 'healthy'."
fi

"${COMPOSE[@]}" exec -T kafka \
    /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null || \
    fail "Kết nối bootstrap tới Kafka thất bại."

echo "Kafka Broker đang khỏe và nhận kết nối bootstrap thành công."
