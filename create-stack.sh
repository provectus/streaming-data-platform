#!/bin/bash

usage() { 
	cat <<EOM 
	Usage: $0 [-m] [-p] [-c] [-s stackname] -r resourceBucket

	-m             Run 'maven clean package' before packaging
	-p             Only package, do not deploy
	-c             Copy resources (ml model, flink jar) to resource folder
	-s stackname   Required when deployed app (without -p). Stack 
	               name. Used also for generate many different resources
	               in the stack
	-r resourceBucket Mandatoty parameter. Points to S3 bucket where resources
	               will be deploying while the stack creating process
EOM
	exit 1;
}

PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
AWS_CLI=$(which aws)

if [ $? != 0 ]; then
	echo "AWS CLI not found. Exiting."
	exit 1
fi

while getopts "mr:s:pch" o; do
    case "${o}" in
        m)
			maven=1
            ;;
        r)
            resourceBucket=${OPTARG}
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
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${resourceBucket}" ]; then
	echo "ERROR: -r is required parameter"
    usage
fi

if [ ! -z "${maven}" ]; then
	echo "Running mvn clean package"
	mvn clean package
else
	echo "Skiping 'maven clean package' (add -m if you want it)"
fi

echo Packaging fds-template.yaml to fds.yaml with bucket ${resourceBucket}

$AWS_CLI cloudformation package  \
	--template-file ${PROJECT_DIR}/fds-template.yaml \
	--s3-bucket ${resourceBucket} \
	--output-template-file ${PROJECT_DIR}/fds.yaml

if [ ! -z "${copyResources}" ]; then
	echo "Copying model.tar.gz to s3://${resourceBucket}/model.tar.gz"
	aws s3 cp ${PROJECT_DIR}/fds-lambda-ml-integration/src/main/resources/model.tar.gz \
		s3://${resourceBucket}/model.tar.gz

	echo "Copying fds-flink-streaming-1.0-SNAPSHOT.jar to s3://${resourceBucket}/fds-flink-streaming.jar"
	aws s3 cp ${PROJECT_DIR}/fds-flink-streaming/target/fds-flink-streaming-1.0-SNAPSHOT.jar \
		s3://${resourceBucket}/fds-flink-streaming.jar
fi

if [ -z "${onlyPackage}" ]; then
	echo "Deploying to the stack ${stackname} ..."

	if [ -z ${stackname} ]; then
		echo
		echo "	**** ERROR: stackname is mandatory when package deployed ****" 2>&1
		echo
		usage
		exit 1
	fi

	aws cloudformation deploy \
		--s3-bucket ${resourceBucket} \
		--template-file ${PROJECT_DIR}/fds.yaml \
		--capabilities CAPABILITY_IAM \
		--parameter-overrides \
		ServicePrefix=$stackname \
		AnalyticalDBName=$stackname \
		S3BucketName=fds$stackname \
		S3ResourceBucket=${resourceBucket} \
		--stack-name $stackname
	
	aws cloudformation \
		describe-stacks --stack-name $stackname
fi
