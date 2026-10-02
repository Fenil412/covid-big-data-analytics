#!/bin/bash
# Legacy entry point — delegates to start-cluster.sh (HDFS + YARN + Spark).
exec bash "$(dirname "${BASH_SOURCE[0]}")/start-cluster.sh"
