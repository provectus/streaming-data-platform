package com.provectus.fds.ml;

class TestAppConstants {

    private TestAppConstants() {}

    static final String ATHENA_QUERY = "select\n" +
            "  case i.txid when null then 0 else 1 end positive,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast(b.campaign_item_id as varchar)))), 9223372036854775807) / 9223372036854775807.0 as campaign_item_id,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.domain))), 9223372036854775807) / 9223372036854775807.0 as domain,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_id))), 9223372036854775807) / 9223372036854775807.0 as creative_id,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(b.creative_category))), 9223372036854775807) / 9223372036854775807.0 as creative_category,\n" +
            "  bitwise_and(from_big_endian_64(xxhash64(to_utf8(cast(b.win_price as varchar)))), 9223372036854775807) / 9223372036854775807.0 as win_price\n" +
            "from bcns b tablesample bernoulli(100) \n" +
            "  left join impressions i on i.txid = b.txid\n" +
            "  where type = 'bid' -- and \n" +
            "    -- b.day >= round((to_unixtime(current_timestamp) - 86400) / 86400) and \n" +
            "    -- i.day >= round((to_unixtime(current_timestamp) - 86400) / 86400)\n" +
            "  limit 30";

    static final String ATHENA_OUTPUT_LOCATION = "s3://newfdsb/athena/";
    static final long SLEEP_AMOUNT_IN_MS = 1000;
    static final String ATHENA_DEFAULT_DATABASE = "default";
    static final int CLIENT_EXECUTION_TIMEOUT = 0;
}
