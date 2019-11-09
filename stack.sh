#!/bin/bash

set -ex

usage() { 
    cat <<EOM
    Usage: $0 [-m] [-p] [-c] [-d] [-t] [-s stackname] [-v version] -b resourceBucket -r region

    -m             Run 'maven clean package' before packaging
    -p             Only package, do not deploy
    -c             Copy resources (ml model, flink jar) to resource folder
    -t             Add timestamp/commit_id to version
    -d             Drop/create resource bucket
    -s stackname   Required when deployed app (without -p). Stack
                   name. Used also for generate many different resources
                   in the stack
    -v version     version of platform
    -b resourceBucket Mandatoty parameter. Points to S3 bucket where resources
                   will be deploying while the stack creating process
    -r region      Region name
EOM
    exit 1;
}

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
AWS_CLI=$(which aws)

cd "$PROJECT_DIR" || exit

if [[ $? != 0 ]]; then
    echo "AWS CLI not found. Exiting."
    exit 1
fi

while getopts "mb:s:pcthdr:v:" o; do
    case "${o}" in
        m)
            maven=1
            ;;
        b)
            resourceBucket=${OPTARG}
            ;;
        r)
            regionName=${OPTARG}
	          ;;
        s)
            stackname=${OPTARG}
            ;;
        p)
            onlyPackage=1
            ;;
        c)
            copyResources=1
            ;;
        t)
            addTimestamp=1
            ;;
        d)
            dropCreateResourceBucket=1
            ;;
        v)
            platformVersion=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [[ -z "${resourceBucket}" ]]; then
    echo "ERROR: -b is required parameter"
    usage
fi

if [[ -z "${platformVersion}" ]]; then
  echo "ERROR: -v is required parameter"
  usage
fi

if [[ ${addTimestamp} -eq 1 ]]; then
  platformVersion=${platformVersion}-$(date +%s)
  if [[ ! -z $CIRCLE_SHA1 ]]; then
    platformVersion=${platformVersion}-$CIRCLE_SHA1
  fi
fi

if [[ ! -z "${maven}" ]]; then
    echo "Running mvn clean package"
    mvn clean package
fi

if [[ -n "${dropCreateResourceBucket}" ]]; then
    echo "Dropping bucket ${resourceBucket}"
    ${AWS_CLI} s3 rb s3://${resourceBucket} --force

    echo "Creating bucket ${resourceBucket} "
    ${AWS_CLI} s3 mb s3://${resourceBucket} --region ${regionName}
fi

echo Preprocess templates to set valid resource bucket values

function preprocess() {
    source="$1-template.yaml"
    target="$1.yaml"

    sed \
      -e "s/@S3ResourceBucket@/${resourceBucket}/" \
      -e "s/@PlatformVersion@/${platformVersion}/" \
      $PROJECT_DIR/$source > $PROJECT_DIR/$target
}

preprocess ml
preprocess processing

echo Packaging fds-template.yaml to fds.yaml with bucket ${resourceBucket}

${AWS_CLI} cloudformation package  \
    --template-file ${PROJECT_DIR}/fds-template.yaml \
    --s3-bucket ${resourceBucket} \
    --s3-prefix ${platformVersion} \
    --output-template-file ${PROJECT_DIR}/fds.yaml

echo "Copying fds.yaml to s3://${resourceBucket}/${platformVersion}/fds.template"
${AWS_CLI} s3 cp ${PROJECT_DIR}/fds.yaml \
    s3://${resourceBucket}/${platformVersion}/fds.template

if [[ ! -z "${copyResources}" ]]; then
    echo "Copying model.tar.gz to s3://${resourceBucket}/${platformVersion}/model.tar.gz"
    ${AWS_CLI} s3 cp ${PROJECT_DIR}/fds-lambda-ml-integration/src/main/resources/model.tar.gz \
        s3://${resourceBucket}/${platformVersion}/model.tar.gz

    echo "Copying fds-flink-streaming-1.0-SNAPSHOT.jar to s3://${resourceBucket}/${platformVersion}/fds-flink-streaming.jar"
    ${AWS_CLI} s3 cp ${PROJECT_DIR}/fds-flink-streaming/target/fds-flink-streaming-1.0-SNAPSHOT.jar \
        s3://${resourceBucket}/${platformVersion}/fds-flink-streaming.jar
fi

if [[ -z "${onlyPackage}" ]]; then
    echo "Deploying to the stack ${stackname} ..."

    if [[ -z ${stackname} ]]; then
        echo
        echo "	**** ERROR: stackname is mandatory when package deployed ****" 2>&1
        echo
        usage
        exit 1
    fi

    service_prefix="sdp-$(cat /dev/urandom | tr -dc 'a-z' | fold -w 7 | head -n 1)"

    ${AWS_CLI} --region ${regionName} cloudformation deploy \
        --s3-bucket ${resourceBucket} \
        --s3-prefix ${platformVersion} \
        --template-file ${PROJECT_DIR}/fds.yaml \
        --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND \
        --stack-name ${stackname} \
        --parameter-overrides ServicePrefix=${service_prefix}

    ${AWS_CLI} --region ${regionName} cloudformation \
        describe-stacks --stack-name ${stackname}
fi

cd - || exit 1
