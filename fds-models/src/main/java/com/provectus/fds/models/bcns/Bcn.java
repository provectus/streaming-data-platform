package com.provectus.fds.models.bcns;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.IOException;

public interface Bcn {
    @JsonIgnore
    String getPartitionKey();
    @JsonIgnore
    byte[] getBytes() throws IOException;
}
