#!/bin/bash

SALT=$2
GOAL=$1

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ $GOAL == "mvn" ]]
	then OPTS="-m"
fi

$PROJECT_DIR/stack.sh $OPTS -r fds-lambda-java -s $SALT

