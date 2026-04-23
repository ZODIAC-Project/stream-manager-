#!/bin/bash

usage() {
cat <<'USAGE'
Usage: curl ... | bash -s -- <image-name> <k8s-file> [container-name]

Description:
    Builds a Docker image using the current Git commit as a temporary tag,
    pushes the image to the registry, retrieves the image digest, and updates
    the given Kubernetes YAML file to reference the image by its digest.

Arguments:
    <image-name>   e.g. myrepo/myimage
    <k8s-file>     path to the Kubernetes YAML file to update
    [container-name]  (optional) container name in the YAML to target

Options:
    -h, --help     Show this help message and exit
    -f, --dockerfile <path>  Use an alternative Dockerfile (default: Dockerfile)
    -u, --username <user>     Docker registry username (optional)
    -t, --token <token>       Registry token/password (use with care)
    --token-file <path>       Read registry token/password from file (safer)

Notes:
    - You must be logged in to your Docker registry before running this script.
    - The script expects to be run from within a Git repository (uses git SHA).
    - The sed replacement is simplistic; ensure the YAML contains a line like
        "image: <image-name>..." so it can be replaced correctly.
USAGE
}

# Print the usage info if requested 
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
        usage
        exit 0
fi

# Default Dockerfile path
DOCKERFILE_PATH="Dockerfile"

# Optional registry credentials (can be provided via CLI flags or env)
REGISTRY_USER=""
REGISTRY_TOKEN=""
TOKEN_FILE=""

# Parse options
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        -f|--dockerfile)
            shift
            if [ -z "$1" ]; then
                echo "Error: --dockerfile requires a path argument"
                exit 1
            fi
            DOCKERFILE_PATH="$1"
            shift
            ;;
        -u|--username)
            shift
            if [ -z "$1" ]; then
                echo "Error: --username requires a value"
                exit 1
            fi
            REGISTRY_USER="$1"
            shift
            ;;
        -t|--token)
            shift
            if [ -z "$1" ]; then
                echo "Error: --token requires a value"
                exit 1
            fi
            REGISTRY_TOKEN="$1"
            shift
            ;;
        --token-file)
            shift
            if [ -z "$1" ]; then
                echo "Error: --token-file requires a path"
                exit 1
            fi
            TOKEN_FILE="$1"
            shift
            ;;
        
        --)
            shift
            break
            ;;
        -*)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

IMAGE_NAME=$1      # z.B. myrepo/myimage
K8S_FILE=$2        # z.B. deployment.yaml
CONTAINER_NAME=$3  # use container name in yaml 

#print user input for human error validation 
echo " _______________________________________________"
echo "| Input:"
echo "| Image Name: $IMAGE_NAME"
echo "| Kubernetes YAML File: $K8S_FILE"
if [ -n "$CONTAINER_NAME" ]; then
    echo "| Container Name in YAML: $CONTAINER_NAME"
fi
echo "| Dockerfile Path: $DOCKERFILE_PATH"
echo "| Username: ${REGISTRY_USER:-(not set)}"
if [ -n "$TOKEN_FILE" ]; then
    echo "| Token File: $TOKEN_FILE"
elif [ -n "$REGISTRY_TOKEN" ]; then
    echo "| Token: (provided via CLI)"
else
    echo "| Token: (not set)"
fi  

echo "|_______________________________________________"
echo "|---> Starte Build-Prozess für $IMAGE_NAME"

GIT_SHA=$(git rev-parse --short HEAD)
FULL_IMAGE_NAME="${IMAGE_NAME}:${GIT_SHA}"

# check if file is present
if [ ! -f "$DOCKERFILE_PATH" ]; then
    echo "Error: No Dockerfile found at '$DOCKERFILE_PATH' in $(pwd). Place a Dockerfile there or pass -f /path/to/Dockerfile."
    exit 1
fi

# If a token file was provided, read it now (safer than passing token on CLI)
if [ -n "$TOKEN_FILE" ]; then
    if [ -f "$TOKEN_FILE" ]; then
        REGISTRY_TOKEN=$(sed -n '1p' "$TOKEN_FILE")
    else
        echo "| Error: token file '$TOKEN_FILE' not found"
        exit 1
    fi
fi

# If a registry token (or password) was provided, attempt docker login for the registry
# Extract registry host portion from the image name (host is before first '/')
REGISTRY_HOST="$(echo "$IMAGE_NAME" | awk -F'/' '{print $1}')"
if [ "$REGISTRY_HOST" = "$IMAGE_NAME" ]; then
    # no explicit registry host present
    REGISTRY_HOST=""
fi

if [ -n "$REGISTRY_TOKEN" ] && [ -n "$REGISTRY_HOST" ]; then
    echo "| Info: attempting docker login to $REGISTRY_HOST"
    # prefer explicit registry user, then DOCKER_USERNAME env, then local $USER
    LOGIN_USER="${REGISTRY_USER:-${DOCKER_USERNAME:-$USER}}"
    if echo "$REGISTRY_TOKEN" | docker login "$REGISTRY_HOST" -u "$LOGIN_USER" --password-stdin > /dev/null 2>&1; then
        echo "| Info: docker login succeeded against $REGISTRY_HOST"
        logged_in=true
    else
        echo "| Error: docker login to $REGISTRY_HOST failed (push may fail)."
    fi
fi

# Ensure logged in to Docker
# Old check relied on `docker info` printing a 'Username:' field which is
# not present on some Docker installations. Instead, check ~/.docker/config.json
# for auths/credsStore/credHelpers and fall back to the old docker info test.
DOCKER_CONFIG_PATH="${DOCKER_CONFIG:-$HOME/.docker/config.json}"
logged_in=false
if [ -f "$DOCKER_CONFIG_PATH" ]; then
    if grep -q '"auths"[[:space:]]*:' "$DOCKER_CONFIG_PATH" 2>/dev/null || \
       grep -q '"credsStore"' "$DOCKER_CONFIG_PATH" 2>/dev/null || \
       grep -q '"credHelpers"' "$DOCKER_CONFIG_PATH" 2>/dev/null; then
        logged_in=true
    fi
fi
# fallback: older Docker versions put Username in `docker info`
if ! $logged_in; then
    if docker info 2>/dev/null | grep -q '^Username:'; then
        logged_in=true
    fi
fi
if ! $logged_in; then
    echo "|Warning: docker does not appear to be logged in. Please run 'docker login' if pushing to a remote registry."
fi

BUILD_LOG=$(mktemp /tmp/image-build-log.XXXXXX)
echo "| Building (quiet). Build output logged to $BUILD_LOG"
BUILD_CONTEXT=$(dirname "$DOCKERFILE_PATH")
if [ -z "$BUILD_CONTEXT" ]; then
    BUILD_CONTEXT='.'
fi

if ! docker build --platform linux/amd64 -f "$DOCKERFILE_PATH" -t "$FULL_IMAGE_NAME" "$BUILD_CONTEXT" --provenance=false > "$BUILD_LOG" 2>&1; then
    echo "| Error: docker build failed. Showing build output (first 500 lines):"
    sed -n '1,500p' "$BUILD_LOG" || true
    rm -f "$BUILD_LOG"
    exit 1
fi
rm -f "$BUILD_LOG"
echo "| Docker image built: $FULL_IMAGE_NAME"
echo "|---> Pushing image to registry and retrieving digest... "

# Push and capture output to extract digest if available
PUSH_OUTPUT=$(docker push "$FULL_IMAGE_NAME" 2>&1) || {
    echo "|Error: docker push failed. Output:";
    echo "|$PUSH_OUTPUT";
    exit 1
}

# Determine the pushed image digest in a robust way.
# Prefer docker inspect RepoDigests (returns 'repo@sha256:...').
# Fallbacks: explicit 'digest: sha256:...' in push output, then last sha256 token.
REPO_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "$FULL_IMAGE_NAME" 2>/dev/null || true)
if [[ -n "$REPO_DIGEST" && "$REPO_DIGEST" == *@sha256:* ]]; then
    DIGEST_FULL="${REPO_DIGEST#*@}"
    DIGEST="${DIGEST_FULL#sha256:}"
else
    # try to find an explicit 'digest: sha256:...' (some registries print this)
    PUSH_DIGEST=$(echo "$PUSH_OUTPUT" | grep -oE 'digest:\s*sha256:[a-f0-9]+' | head -n1 | sed 's/.*digest:\s*//') || true
    if [[ -n "$PUSH_DIGEST" ]]; then
        DIGEST_FULL="$PUSH_DIGEST"
        DIGEST="${DIGEST_FULL#sha256:}"
    else
        # last resort: pick the last sha256:... token from the push output
        LAST_SHA=$(echo "$PUSH_OUTPUT" | grep -oE 'sha256:[a-f0-9]+' | tail -n1 || true)
        if [[ -n "$LAST_SHA" ]]; then
            DIGEST_FULL="$LAST_SHA"
            DIGEST="${DIGEST_FULL#sha256:}"
        else
            DIGEST=""
        fi
    fi
fi

if [ -z "$DIGEST" ]; then
    echo "|Error: Could not retrieve digest. Was the image pushed?"
    echo "|Try: docker inspect --format='{{index .RepoDigests}}' $FULL_IMAGE_NAME"
    exit 1
fi

echo "| Found digest: $DIGEST"
echo "|---> Updating Kubernetes YAML file: $K8S_FILE"
NEW_IMAGE_REFERENCE="${IMAGE_NAME}@${DIGEST_FULL}"

# Perform a global image replacement in the YAML file (portable sed)
if [ ! -f "$K8S_FILE" ]; then
    echo "| Error: target YAML file '$K8S_FILE' does not exist. Aborting replacement."
    exit 1
fi
grep -- "| image: ${IMAGE_NAME}" "$K8S_FILE" || echo "| (no direct 'image: ${IMAGE_NAME}' matches found)"
grep -- "| image:" "$K8S_FILE" | head -n 10 || true

REPLACEMENT_CMD_STATUS=0
# Compute image basename (e.g. 'data_consumer' from 'mathiskae/data_consumer')
IMAGE_BASENAME="${IMAGE_NAME##*/}"

# Use a portable awk-based replacement that writes to a temp file and moves it into place.
TMPFILE="$(mktemp "${K8S_FILE}.tmp.XXXXXX")"
echo "|Using portable awk replacement (matching basename='${IMAGE_BASENAME}'), writing to $TMPFILE"
awk -v basename="$IMAGE_BASENAME" -v newref="$NEW_IMAGE_REFERENCE" '
  { if ($0 ~ /^[[:space:]]*image:/ && $0 ~ basename && !replaced) {
        match($0,/^[[:space:]]*/);
        lead=substr($0,RSTART,RLENGTH);
        print lead "image: " newref;
        replaced=1;
    } else {
        print $0;
    } }
' "$K8S_FILE" > "$TMPFILE" || REPLACEMENT_CMD_STATUS=$?

if [ "$REPLACEMENT_CMD_STATUS" -eq 0 ]; then
    # Only move the temp file over the real file if awk succeeded
    mv "$TMPFILE" "$K8S_FILE" || { echo "| Error: could not move temp file to $K8S_FILE"; REPLACEMENT_CMD_STATUS=1; }
else
    echo "| Replacement awk failed (exit $REPLACEMENT_CMD_STATUS). Leaving original file untouched."
    rm -f "$TMPFILE" 2>/dev/null || true
fi

echo "|                              "
echo "|---> Verifying replacement results (first matching lines):"
grep -- "| ${NEW_IMAGE_REFERENCE}" "$K8S_FILE" | head -n 20 || echo "|  (no lines found containing the new image reference)"

if ! grep -q -- "${NEW_IMAGE_REFERENCE}" "$K8S_FILE"; then
    echo "| Replacement did not succeed."


    echo "| Attempting fallback: replace first 'image:' line with the new image reference (safe for single-deployment files)"
    if awk -v newref="${NEW_IMAGE_REFERENCE}" 'BEGIN{done=0} {
          if(!done && $0 ~ /^[[:space:]]*image:/){
              match($0,/^[[:space:]]*/);
              lead=substr($0,RSTART,RLENGTH);
              print lead "image: " newref;
              done=1;
          } else print $0;
        }' "$K8S_FILE" > "$K8S_FILE.tmp"; then
        mv "$K8S_FILE.tmp" "$K8S_FILE"
        echo "| Fallback replacement performed — verifying results..."
        if grep -- "| ${NEW_IMAGE_REFERENCE}" "$K8S_FILE" | head -n 20; then
            echo "| Fallback replacement succeeded."
        else
            echo "| Fallback did not produce the expected. Check the file $K8S_FILE manually.";
        fi
    else
        echo "| Fallback replacement failed (awk error)."
    fi
else
    echo "| Replacement succeeded. YAML file $K8S_FILE has been updated with the digest."
    echo "|_____________________________________________"
fi
echo 
echo "----> Ready for: kubectl apply -f $K8S_FILE"