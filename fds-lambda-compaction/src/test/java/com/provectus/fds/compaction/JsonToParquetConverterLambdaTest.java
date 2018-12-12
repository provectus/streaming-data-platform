package com.provectus.fds.compaction;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JsonToParquetConverterLambdaTest {
    @Test
    public void testPathConverter() {
        JsonToParquetConverterLambda app = new JsonToParquetConverterLambda();
        String result = app.convertPath("/raw/bcns/2018/12/11/test.gz");

        assertEquals("/parquet/bcns/year=2018/month=12/day=11/test.parquet", result);
    }

    @Test
    public void testPathConverterSecond() {
        JsonToParquetConverterLambda app = new JsonToParquetConverterLambda();
        String result = app.convertPath("/raw/bcns/2018/12/11/test.gz", "data.parquet");

        assertEquals("/parquet/bcns/year=2018/month=12/day=11/data.parquet", result);
    }
}
