package com.provectus.fds.it;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.asynchttpclient.AsyncHttpClient;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class SampleMain {

    final static AsyncHttpClient httpClient = asyncHttpClient(config());
    final static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws JsonProcessingException {
        System.out.println("Starting generate sample data");
        SampleDataResult result = new SampleGenerator(50, 60,
                "https://x4e3w5eif9.execute-api.us-west-2.amazonaws.com/integration8009b5224f95441ab22",
                httpClient, objectMapper).generateSampleData();

        System.out.println("Data generation finished");
    }
}
