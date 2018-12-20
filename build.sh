#! /bin/bash
mvn clean package
aws cloudformation package  --template-file fds-template.yaml --s3-bucket fds-lambda-java --output-template-file fds.yaml
aws cloudformation deploy --template-file /Users/rustam/work/squadex-fastdata-solution/fds.yaml --capabilities CAPABILITY_IAM --stack-name fds-snapshot
