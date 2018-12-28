#! /bin/bash

CID=$RANDOM
BASE_URL=`aws cloudformation  describe-stacks --stack-name $2 | jq -r '.Stacks[0].Outputs[] | select(.OutputKey | contains("RootUrl")).OutputValue'`

BIDS=0
IMPS=0
CLICKS=0

for i in `seq 1 $1`
	do
	txid=$RANDOM
	res=`curl --write-out %{http_code} -s $BASE_URL/bids?appuid=123\&txid=${txid}\&campaign_item_id=${CID}\&domain=example.com\&creative_category=test\&creative_id=some`
	BIDS=$(( $BIDS+$(( 0+$([[ "$res" -eq 200 ]] && echo 1 || echo 0) )) ))
	res=`curl --write-out %{http_code} -s $BASE_URL/impressions?appuid=123\&txid=${txid}\&win_price=100`
	IMPS=$(( $IMPS+$(( 0+$([[ "$res" -eq 200 ]] && echo 1 || echo 0) )) ))
	res=`curl --write-out %{http_code} -s $BASE_URL/clicks?appuid=123\&txid=${txid}\&campaign_item_id=${CID}`
	CLICKS=$(( $CLICKS+$(( 0+$([[ "$res" -eq 200 ]] && echo 1 || echo 0) )) ))
done

echo BIDS: $BIDS, IMPS: $IMPS, CLICKS: $CLICKS
