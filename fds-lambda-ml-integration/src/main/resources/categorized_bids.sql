select
  case when i.tx_id is null then 0 else 1 end positive,
  bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast(b.campaign_item_id as varchar)))), 9223372036854775807) / 9223372036854775807.0 as campaign_item_id,
  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.domain))), 9223372036854775807) / 9223372036854775807.0 as domain,
  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_id))), 9223372036854775807) / 9223372036854775807.0 as creative_id,
  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_category))), 9223372036854775807) / 9223372036854775807.0 as creative_category,
  coalesce(i.win_price, 0) as win_price
from parquet_bcns b TABLESAMPLE BERNOULLI(100)
  left join parquet_impressions i on i.tx_id = b.tx_id
  where b.type = 'bid' and
    b.day >= round((to_unixtime(current_timestamp) - 86400) / 86400)