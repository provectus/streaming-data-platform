package com.provectus.fds.ml;

import org.junit.Test;

import static org.junit.Assert.*;

public class EndpointUpdaterTest {

    @Test
    public void testGeneratedNames() {

        EndpointUpdater.EndpointUpdaterBuilder builder
                = new EndpointUpdater.EndpointUpdaterBuilder(new ReplaceEndpointConfigLambda.LambdaConfiguration());

        builder.withServicePrefix("0123456789");
        String generatedName
                = builder.generateName("EndpointConfiguration");

        assertTrue(generatedName.contains("EndpointConfiguration"));
        assertEquals(63, generatedName.length());
    }
}