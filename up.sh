#!/bin/bash

set -euo pipefail
rsync -az --delete --filter '. .rsync-filter' . "$SYNCSCRIPT_RSYNC_REMOTE"
