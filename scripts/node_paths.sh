#!/usr/bin/env bash
# Shared paths for multi-node layout. Source after setting SERVICE_NAME and REPO_ROOT.
#
# Layout:  $NODES_ROOT/$SERVICE_NAME/subtensor
#           $NODES_ROOT/$SERVICE_NAME/config/
# Data:     /var/lib/$SERVICE_NAME

: "${SERVICE_NAME:?SERVICE_NAME required}"
: "${REPO_ROOT:?REPO_ROOT required}"

NODES_ROOT="${SUBTENSOR_NODES_ROOT:-$REPO_ROOT/nodes}"
SERVICE_DIR="$NODES_ROOT/$SERVICE_NAME"
BIN_DEST="$SERVICE_DIR/subtensor"
CONFIG_DIR="$SERVICE_DIR/config"
ENV_DEST="$CONFIG_DIR/subtensor.env"
RUN_SCRIPT="$CONFIG_DIR/run_node.sh"
BUILD_BINARY="$REPO_ROOT/target/production/node-subtensor"
BASE_PATH="/var/lib/$SERVICE_NAME"
TEMPLATE_ENV="$REPO_ROOT/config/subtensor.env.example"
TEMPLATE_CONFIG="$REPO_ROOT/config"
