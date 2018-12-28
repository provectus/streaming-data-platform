package com.provectus.fds.reports;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.provectus.fds.models.utils.JsonUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ApiHandler implements RequestStreamHandler {
    Map<ExecutionContext, Function<ExecutionValues,Object>> handlerMap = new HashMap<>();

    public ApiHandler() {
        AggregationsReportHandler aggregationsHandler = new AggregationsReportHandler();

        this.handlerMap.put(
                new ExecutionContext("GET", "/reports/campaigns/:campaign_item_id"),
                aggregationsHandler::getTotal
        );

        this.handlerMap.put(
                new ExecutionContext("GET", "/reports/campaigns/:campaign_item_id/period"),
                aggregationsHandler::getByPeriod
        );
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        ExecutionValues executionValues = JsonUtils.read(reader, ExecutionValues.class);
        context.getLogger().log("Received event: "+executionValues.toString());
        ExecutionContext executionContext = new ExecutionContext(executionValues);
        Function<ExecutionValues,Object> handler = this.handlerMap.get(executionContext);
        if (handler!=null) {
            outputStream.write(JsonUtils.write(handler.apply(executionValues)));
        }
    }
}
