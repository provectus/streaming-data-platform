package com.provectus.fds.compaction;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;

import java.util.stream.Collectors;

public class JsonNewLineAdderLambda implements RequestHandler<KinesisFirehoseEvent, KinesisFirehoseResponse> {

    @Override
    public KinesisFirehoseResponse handleRequest(KinesisFirehoseEvent kinesisFirehoseEvent, Context context) {
        context.getLogger().log("Received: "+kinesisFirehoseEvent.toString());
        KinesisFirehoseResponse response =  new KinesisFirehoseResponse(
                kinesisFirehoseEvent.getRecords()
                    .stream().map(
                            r -> KinesisFirehoseResponse.FirehoseRecord.appendNewLine(r.getRecordId(), r.getData())
                    ).collect(Collectors.toList())
        );
        context.getLogger().log("Added lines to "+response.size()+" events");

        return response;
    }
}
