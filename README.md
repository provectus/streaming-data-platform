# FastData solution
1. [About](#about)
    1. [Architecture](#architecture)
    1. [Data Types](#data-types)
    1. [Region availability](#region-availability)
1. [Developing](#developing)
    1. [Build](#build)
    1. [Deploy](#deploy)
    1. [Test](#test)
        1. [Integration](#integration)
        1. [Performance](#performance)
    1. [Populate with data](#populate-with-data)
## About
### Architecture
![diagram.svg](images/diagram.svg)
### Region availability
| Services          | Northern Virginia | Ohio | Oregon | Northern California | Montreal | São Paulo | Ireland | Frankfurt | London | Paris | Stockholm | Singapore | Tokyo | Sydney | Seoul | Mumbai |
|-------------------|-------------------|------|--------|---------------------|----------|-----------|---------|-----------|--------|-------|-----------|-----------|-------|--------|-------|--------|
| Api Gateway       | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| DynamoDB          | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| Glue              | ✓                 | ✓    | ✓      | ✓                   | ✓        |           | ✓       | ✓         | ✓      |       |           | ✓         | ✓     | ✓      | ✓     | ✓      |
| IAM               | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| Kinesis Streams   | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| Kinesis Analytics | ✓                 | ✓    | ✓      |                     |          |           | ✓       | ✓         |        |       |           |           |       |        |       |        |
| Kinesis Firehose  | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     |           | ✓         | ✓     | ✓      | ✓     | ✓      |
| Lambda            | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| S3                | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      | ✓     | ✓         | ✓         | ✓     | ✓      | ✓     | ✓      |
| Serverless        | ✓                 | ✓    | ✓      | ✓                   | ✓        | ✓         | ✓       | ✓         | ✓      |       |           | ✓         | ✓     | ✓      | ✓     | ✓      |
## Developing
### Build
```
mvn clean package
```
### Deploy
Before deploying need to upload artifacts to S3
```
aws cloudformation package
    --template-file fds-template.yaml
    --s3-bucket 
    --output-template-file fds.yaml
```
After that all builded artifacts would be stored in S3 bucket named <s3-bucket-name>
```
aws cloudformation deploy
    --template-file fds.yaml
    --capabilities CAPABILITY_IAM
    [ --parameter-overrides ServicePrefixName=<some-value> AnalyticalDBName=<some-value> S3Bucket=<some-value> AggregationPeriod=10 ]
    --stack-name <stack-name>
```
After creating stack would be provided are outputs:
- `UrlForAPI` - URL for incoming bid requests
- `UrlForReports` - URL for reports
### Test
#### Integration
```
mvn -fn verify
```
Test report would be stored in `./fds-it/target/surefire-reports/`
#### Performance
For test purposes we chosen [gatling](https://gatling.io)
```
docker run -it --rm -e JAVA_OPTS="-Dduration=60 -DbaseUrl=<UrlForAPI> -Dgatling.core.simulationClass=basic.ApiPerformanceTest" -v  `pwd`/gatling:/opt/gatling/user-files -v `pwd`/gatling/results:/opt/gatling/results -v `pwd`/gatling/conf:/opt/gatling/conf denvazh/gatling 
```
Test report would be stored in `./gatling/results/`
### Populate with data
For manually performing basic bid requests:
```
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473","domain":"www.google.com","appuid":"e582f2a0-3e2b-4066-a2a3-dc5867953d0d","campaign_item_id":1463517,"creative_id":"b72897cb-3f88-423b-84aa-9b7710d2416d","creative_category":"testCreativeCategory"}' '<UrlForAPI>/bid'
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473"}' '<UrlForAPI>/click'
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473","win_price":1}' '<UrlForAPI>/impression'
```
The following request for retrieving report for each campaign item:
```$xslt
curl -o bid-report.json '<UrlForReports>/reports/campaigns/<campaign_item_id>'
``` 