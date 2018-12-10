# squadex-fastdata-solution

### Build

```
mvn clean package
```

### Package

```
aws cloudformation package \
    --template-file cloudformation.template  \
    --s3-bucket fds-lambda-java8 \
    --output-template-file packaged-template.yaml
```

### Deploy
```
aws cloudformation deploy \
    --template-file packaged-template.yaml \
    --capabilities CAPABILITY_IAM \
    --stack-name fds-snapshot
```

