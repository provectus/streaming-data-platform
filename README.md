# squadex-fastdata-solution

### Build

```
mvn clean package
```

### Package

```
aws cloudformation package
    --template-file fds-template.yaml
    --s3-bucket fds-lambda-java
    --output-template-file fds.yaml
```

### Deploy
```
aws cloudformation deploy
    --template-file fds.yaml
    --capabilities CAPABILITY_IAM
    [ --parameter-overrides ServicePrefix=demo AnalyticalDBName=fds S3Bucket=demo VPCCIDR=10.1.10.2/24 ]
    --stack-name <stack-name>
```

### Populate with data
```
test.sh <number-of-requests> <stack-name>
```
