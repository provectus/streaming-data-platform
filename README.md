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
    [ --parameter-overrides FDSServicePrefix=fds FDSAnalyticalDatabaseName=fds  FDSS3Bucket=fds ]
    --stack-name <stack-name>
```
