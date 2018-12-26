#! /bin/bash

CID=$RANDOM
BASE_URL=`aws cloudformation  describe-stacks --stack-name fds$1 | jq -r '.Stacks[0].Outputs[] | select(.OutputKey | contains("RootUrl")).OutputValue'`

for i in `seq 1 $2`
	do
	txid=$RANDOM
	curl -v $BASE_URL/bids?appuid=123\&txid=${txid}\&campaign_item_id=${CID}\&domain=example.com\&creative_category=test\&creative_id=some
	curl -v $BASE_URL/impressions?appuid=123\&txid=${txid}\&win_price=100
	curl -v $BASE_URL/clicks?appuid=123\&txid=${txid}\&campaign_item_id=${CID}
done
