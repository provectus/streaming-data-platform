package com.provectus.fds.dynamodb.repositories;

import com.provectus.fds.dynamodb.models.Aggregation;
import org.junit.Test;

import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class AggregationRepositoryTest {

    @Test
    public void getGrouped() {
        AggregationRepository repository = new AggregationRepository();
        List<Aggregation> aggregations = Arrays.asList(
                new Aggregation(-1987217790,1546068600,0L,0L,17986L)
        );

        List<Aggregation> expected = Arrays.asList(
                new Aggregation(-1987217790,1546041600,0L,0L,17986L)
        );

        List<Aggregation> result = repository.getGrouped(aggregations, ChronoUnit.DAYS, ZoneOffset.UTC, false);
        assertArrayEquals(expected.toArray(), result.toArray());

    }
}