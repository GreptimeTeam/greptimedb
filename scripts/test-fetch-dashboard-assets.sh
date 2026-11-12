#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TEST_DIR=$(mktemp -d)
trap 'rm -rf "$TEST_DIR"' EXIT
mkdir -p "$TEST_DIR/bin" "$TEST_DIR/download"

cat > "$TEST_DIR/bin/curl" <<'STUB'
#!/usr/bin/env bash
printf '%s\n' "$@" >> "$CURL_ARGS_FILE"
while (($#)); do
    if [[ "$1" == --output ]]; then
        touch "$2"
        break
    fi
    shift
done
STUB
for command in shasum sha256sum tar; do
    printf '#!/usr/bin/env bash\nexit 0\n' > "$TEST_DIR/bin/$command"
done
chmod +x "$TEST_DIR/bin/"*

for origin in https://github.com https://mirror.example/https://github.com https://github.com.evil.example http://github.com; do
    for token_source in DASHBOARD_GITHUB_TOKEN GH_TOKEN GITHUB_TOKEN; do
        export CURL_ARGS_FILE="$TEST_DIR/curl-args"
        : > "$CURL_ARGS_FILE"
        env -u DASHBOARD_GITHUB_TOKEN -u GH_TOKEN -u GITHUB_TOKEN \
            "$token_source=test-token" \
            PATH="$TEST_DIR/bin:$PATH" GITHUB_PROXY_URL="$origin" \
            DASHBOARD_RELEASE_VERSION=test-release \
            bash "$SCRIPT_DIR/fetch-dashboard-assets.sh" "$TEST_DIR/download" >/dev/null
        if [[ "$origin" == https://github.com ]]; then
            grep -qx 'Authorization: Bearer test-token' "$CURL_ARGS_FILE"
        elif grep -q 'test-token' "$CURL_ARGS_FILE"; then
            echo "Token sent to $origin" >&2
            exit 1
        fi
    done
done
printf 'Dashboard token origin tests passed\n'
