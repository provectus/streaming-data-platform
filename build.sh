#! /bin/bash
SALT=$2
GOAL=$1
if [[ $GOAL == "mvn" ]]
	then mvn clean package
fi
aws cloudformation package  --template-file fds-template.yaml --s3-bucket fds-lambda-java --output-template-file fds.yaml
aws cloudformation deploy --template-file /Users/rustam/work/squadex-fastdata-solution/fds.yaml --capabilities CAPABILITY_IAM --parameter-overrides FDSServicePrefix=$SALT FDSAnalyticalDatabaseName=$SALT  FDSS3Bucket=fds$SALT --stack-name fds-$SALT
aws cloudformation  describe-stacks --stack-name fds-$SALT
