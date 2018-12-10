package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;

public interface Bcn {
    @JsonIgnore
    String getPartitionKey();
    @JsonIgnore
    String getStreamName();
    @JsonIgnore
    byte[] getBytes() throws IOException;
}
