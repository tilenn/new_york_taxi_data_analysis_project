#!/bin/bash

set -euo pipefail

srun \
    --job-name fri_bd_jupyter \
    --cpus-per-task 1 \
    --mem 8G \
    --gpus 0 \
    --time 1-0 \
    bash -c 'set -euo pipefail
echo "Got the job, activating venv..."
cd -- "$1"
. .venv/bin/activate
echo "Venv activated, getting worker IP..."
IP=$(python3 -c "import socket; print(socket.gethostbyname(socket.gethostname()))")
echo "Scanning for available port..."
AVAILABLE_PORT=""
for PORT in {20000..21000}; do
    if ! (>/dev/tcp/127.0.0.1/"$PORT") &>/dev/null && ! (>/dev/tcp/"$IP"/"$PORT") &>/dev/null; then
        AVAILABLE_PORT=${PORT}
        break
    fi
done
if [[ -z $AVAILABLE_PORT ]]; then
    echo "All ports taken (???)"
    exit 1
fi
echo "Starting Jupyter on port $AVAILABLE_PORT..."
jupyter server --ip "$IP" --port "$AVAILABLE_PORT" --port-retries 100' \
    bash "$PWD"
