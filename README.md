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
### Data Types
The API could receive three data types:
- [Bid](#bid)
- [Click](#click)
- [Impression](#impression)

How to use they described in [this](#populate-with-data) block.

For reporting is available only [Aggregation](#aggregation). User could access reports through Athena or Api Gateway. In case of using Athena available are next tables:
- parquet_aggregates and raw_aggregates
- parquet_bcns and raw_bcns
- parquet_clicks and raw_clicks
- parquet_impressions and raw_impressions

Each table store appropriate data type in defined format: json or [parquet](https://parquet.apache.org/)
##### Bid
```
title: Bid
type: object
properties:
  appuid:
    required: true
    type: string
  campaign_item_id:
    required: true
    type: integer
  creative_category:
    type: string
  creative_id:
    type: string
  txid:
    required: true
    type: string
  domain:
    type: string
  win_price:
    type: integer
  type:
    type: string
    default: bid
```
##### Click
```
title: Click
type: object
properties:
  txid:
    required: true
    type: string
  type:
    type: string
    default: click
```
##### Impression
```
title: Impression
type: object
properties:
  txid:
    required: true
    type: string
  win_price:
    required: true
    type: integer
  type:
    type: string
    default: imp
```
##### Aggregation
```
title: Aggregation
type: object
properties:
  campaign_item_id:
    required: true
    type: integer
  period:
    type: integer
  clicks:
    type: integer
  imps:
    type: integer
  bids:
    type: integer
```
### Region availability
All used services available only in next AWS regions:
- Northern Virginia
- Ohio
- Oregon
- Ireland
- Frankfurt

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
    --s3-bucket <s3-bucket-name>
    --output-template-file fds.yaml
```
After that all builded artifacts would be stored in S3 bucket named `<s3-bucket-name>`
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
By default integration tests are use us-west-2 region.
```
mvn -fn verify
```
Test report would be stored in `./fds-it/target/surefire-reports/`
#### Performance
For test purposes was chosen [gatling](https://gatling.io)
```
docker run -it --rm -e JAVA_OPTS="-Dduration=60 -DbaseUrl=<UrlForAPI> -Dgatling.core.simulationClass=basic.ApiPerformanceTest" -v  `pwd`/gatling:/opt/gatling/user-files -v `pwd`/gatling/results:/opt/gatling/results -v `pwd`/gatling/conf:/opt/gatling/conf denvazh/gatling 
```
Test report would be stored in `./gatling/results/`
### Populate with data
For sent basic requests needs to perform appropriate calls. For example, Bid:
```
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473","domain":"www.google.com","appuid":"e582f2a0-3e2b-4066-a2a3-dc5867953d0d","campaign_item_id":1463517,"creative_id":"b72897cb-3f88-423b-84aa-9b7710d2416d","creative_category":"testCreativeCategory"}' '<UrlForAPI>/bid'
```
Click:
```
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473"}' '<UrlForAPI>/click'
```
Impression:
```
curl --request POST --header "Accept: application/json" --data '{"txid":"44db4cf3-c372-4f7c-8443-9d2a1e725473","win_price":1}' '<UrlForAPI>/impression'
```
The following request could retrieve report for each campaign item:
```$xslt
curl -o bid-report.json '<UrlForReports>/reports/campaigns/<campaign_item_id>'
``` 