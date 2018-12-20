#! /bin/bash

REST_ID=`aws apigateway get-rest-apis | jq -r '.items[] | select(.name="fdsanalytics_FDS_API").id'`
CID=$RANDOM

resources=`aws apigateway get-resources --rest-api-id ${REST_ID}`
bid_resource=`echo $resources | jq -r '.items[] | select(.path | contains("bids")).id'`
impression_resource=`echo $resources | jq -r '.items[] | select(.path | contains("impressions")).id'`
click_resource=`echo $resources | jq -r '.items[] | select(.path | contains("clicks")).id'`

for i in `seq 1 $1`
	do
	txid=$RANDOM
	aws apigateway test-invoke-method --rest-api-id=${REST_ID} --http-method=GET --resource-id=${bid_resource} --path-with-query-string /bids?appuid=123\&txid=${txid}\&campaign_item_id=${CID}\&domain=example.com\&creative_category=test\&creative_id=some
	aws apigateway test-invoke-method --rest-api-id=${REST_ID} --http-method=GET --resource-id=${impression_resource} --path-with-query-string /impressions?appuid=123\&txid=${txid}\&win_price=100
	aws apigateway test-invoke-method --rest-api-id=${REST_ID} --http-method=GET --resource-id=${click_resource} --path-with-query-string /clicks?appuid=123\&txid=${txid}\&campaign_item_id=${CID}
done
