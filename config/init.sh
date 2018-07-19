#!/bin/bash

set -e

export PSQL_USER=postgres

# Set up properties
source config/create-ci-properties-files.sh

createdb bio-test
