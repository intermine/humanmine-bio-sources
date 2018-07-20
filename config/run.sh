#!/bin/bash

set -e

echo "RUNNING FlyMine bio sources unit tests"

export ANT_OPTS='-server'

# install tests
./gradlew install

# run tests
./gradlew build

./gradlew checkstyleMain

./config/parse_checkstyle_report.py 'bio/build/reports/checkstyle/checkstyle_report.xml'
