#!/usr/bin/env bash
# Reproduction launcher for the owned remote experiment, not a portable install.
set -euo pipefail
TASK_ROOT=/data/wuhao.xwh/seekdb-sirius-multigpu-20261027
BASE=/data/wuhao.xwh/seekdb-sirius-20261022
export CUDA_VISIBLE_DEVICES=0,1
export SEEKDB_SIRIUS_CONFIG="$TASK_ROOT/g2-0.075.yaml"
export SEEKDB_SIRIUS_FRAGMENT_LIMIT_MIB=2048
export LD_LIBRARY_PATH="$TASK_ROOT/overlay:/data/wuhao.xwh/duckdb-sirius-upstream/sirius/.pixi/envs/default/lib"
exec "$BASE/venv/bin/python" "$BASE/seekdb/tools/sirius/sidecar.py" \
  --socket /data/wuhao.xwh/seekdb-sirius-20261023/run/sirius.sock \
  --library "$TASK_ROOT/bridge/libseekdb_sirius_bridge.so" \
  --host-pin-mib 64 --native-prefetch --native-chunk-mib 1536 --warmup "$@"
