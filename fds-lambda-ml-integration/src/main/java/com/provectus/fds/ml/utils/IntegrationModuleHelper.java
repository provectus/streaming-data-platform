package com.provectus.fds.ml.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Helper utility class for current module
 */
public class IntegrationModuleHelper {

    /**
     * Proxy method for ObjectMapper which hides all of the checked
     * exception
     *
     * @param o      Object to serialize into String
     * @param mapper Configured ObjectMapper
     * @return JSON String of the object
     */
    public String writeValueAsString(Object o, ObjectMapper mapper) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
