#!/bin/sh
set -e
cd "${0%/*}"
exec ./executor cfg/executor.cfg
