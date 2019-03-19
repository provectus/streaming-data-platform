# AWS Native Streaming Data Platform
1. [Overview](#Overview)
    1. [Architecture](#architecture)
    1. [Region availability](#region-availability)
1. [Developing](#developing)
    1. [Build](#build)
    1. [Deploy](#deploy)
    1. [Test](#test)
        1. [Integration](#integration)
        1. [Performance](#performance)
1. [User Guide](#user-guide)
    1. [Data Ingestion](#data-ingestion)
    1. [Reporting](#reporting)

## Overview
Streaming Data Platform is a unified solution for real time streaming data pipelines, streams powered data lake and data driven microservices across enterprise. 
### Use Cases
The template is designed for the following initiatives:
- Plug-n-play solution for processing and storing data streams in AWS 
- Migration of Hadoop based on-prem platform to AWS native streaming services
- Migration of legacy Enterprise Service Bus or Data Integration architectures to AWS native platform
- Rearchitecture of Data Warehouse workloads to handle growing data volume, velocity as well as to provide capabilities for realtime analytics
- Rearchitecture of slow, inconsistent and always-out-of-date data marts in existing Data Lake or Data Warehouse
- Rearchitecture of duplicated and disjointed realtime and batch pipelines

### Architecture
![diagram.svg](images/diagram.svg)

### How it works

Streaming Data Platform is delivered as cloudformation template quick start. 

Data ingestion layer supports API Gateway endpoint, SFTP, RDS Capture Data Changes, S3 events, DynamoDB, and other connectors. 

All data is streamed in Kinesis, partitioned by business key and split by separate kinesis streams for horizontal scalability. 

Blueprint Kinesis Analytics processor is a point of customization to filter, enrich and aggregate incoming data. 

Ingested and processed messages are stored on S3 with minimum latency. 

Smart partitioning on S3 and columnar format enables subsecond SQL queries from Athena clients. 

Each type of message is registered in AWS Glue with associated metadata for catologization and self-description purposes. 
AWS Athena is an interactive ad-hoc SQL interface on top of these tables. Aggregated and processed data is stored in DynamoDB for online reporting capabilities. 

### Region availability
All services which was used in stack available only in the following AWS regions:
- Northern Virginia (`us-east-1`)
- Ohio (`us-east-2`)
- Oregon (`us-west-2`)
- Ireland (`eu-west-1`)
- Frankfurt (`eu-central-1`)
## Developing
### Build
```
mvn clean package
```
### Deploy
Upload build artifacts to S3
```
aws cloudformation package
    --template-file fds-template.yaml
    --s3-bucket <s3-bucket-name>
    --output-template-file fds.yaml
```
Deploy cloudformation stack:
```
aws cloudformation deploy
    --template-file fds.yaml
    --capabilities CAPABILITY_IAM
    [ --parameter-overrides ServicePrefixName=<some-value> AnalyticalDBName=<some-value> S3Bucket=<some-value> AggregationPeriod=10 ]
    --stack-name <stack-name>
```
Stack outputs:
- `UrlForAPI` - URL for injection requests
- `UrlForReports` - URL for retrieving reports
### Test
#### Integration
The integration test would be launched in `us-west-2` region by default.
```
mvn -fn verify
```
Test report is stored in `./fds-it/target/surefire-reports/`
#### Performance
[Gatling](https://gatling.io) is used for performance testing
```
docker run -it --rm -e JAVA_OPTS="-Dduration=60 -DbaseUrl=<UrlForAPI> -Dgatling.core.simulationClass=basic.ApiPerformanceTest" -v  `pwd`/gatling:/opt/gatling/user-files -v `pwd`/gatling/results:/opt/gatling/results -v `pwd`/gatling/conf:/opt/gatling/conf denvazh/gatling 
```
Test report would be stored in `./gatling/results/`
## User Guide
An architecture is generic enough to support any business use case. For the demo purposes a canonical Adtech use case is implemented.
### Data Ingestion
The following data streams are available:
- [Bid](#bid)
- [Click](#click)
- [Impression](#impression)

##### Bid
Ingesting Bids:
```
curl --request POST --header "Accept: application/json" --data '{"tx_id":"44db4cf3-c372-4f7c-8443-9d2a1e725473","domain":"www.google.com","app_uid":"e582f2a0-3e2b-4066-a2a3-dc5867953d0d","campaign_item_id":1463517,"creative_id":"b72897cb-3f88-423b-84aa-9b7710d2416d","creative_category":"testCreativeCategory"}' '<UrlForAPI>/bid'
```
Bid Schema
```
title: Bid
type: object
properties:
  app_uid:
    required: true
    type: string
  campaign_item_id:
    required: true
    type: integer
  creative_category:
    type: string
  creative_id:
    type: string
  tx_id:
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
Ingesting Clicks:
```
curl --request POST --header "Accept: application/json" --data '{"tx_id":"44db4cf3-c372-4f7c-8443-9d2a1e725473"}' '<UrlForAPI>/click'
```
Click Schema
```
title: Click
type: object
properties:
  tx_id:
    required: true
    type: string
  type:
    type: string
    default: click
```

##### Impression
Ingesting Impressions:
```
curl --request POST --header "Accept: application/json" --data '{"tx_id":"44db4cf3-c372-4f7c-8443-9d2a1e725473","win_price":1}' '<UrlForAPI>/impression'
```

Impression schema:
```
title: Impression
type: object
properties:
  tx_id:
    required: true
    type: string
  win_price:
    required: true
    type: integer
  type:
    type: string
    default: imp
```

### Reporting
Reports are being calculated in Kinesis and stored in `Aggregation` Stream. Aggregated stream is exposed for [realtime consumers](#realtime-reporting) as well as for [offline queries](#analytical-queries).

Aggregation Schema:
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
  impressions:
    type: integer
  bids:
    type: integer
```
#### Realtime Reporting 
Realtime reporting for particular as Ad Campaign is available via API Gateway:

```$xslt
curl -o bid-report.json '<UrlForReports>/reports/campaigns/<campaign_item_id>'
``` 

#### Analytical Queries
Kinesis Streams are snapshotted and compacted in S3 for Data Lake type of workloads.
Each table stores data streams (Bids, Clicks, Impressions and Aggregations) data type in json or [parquet](https://parquet.apache.org/).

The following tables are available for Athena queries:
- parquet_aggregates and raw_aggregates
- parquet_bcns and raw_bcns
- parquet_clicks and raw_clicks
- parquet_impressions and raw_impressions
