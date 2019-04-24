package com.provectus.fds.ml;

import com.amazonaws.services.athena.model.Row;

/**
 * Base interface for record processing
 */
public interface RecordProcessor {

    /**
     * Initialize all internal structure if needed
     * @throws Exception
     */
    void initialize() throws Exception;

    /**
     * Process single row
     *
     * @param row
     * @throws Exception
     */
    void process(Row row) throws Exception;

    /**
     * Complete called after all records have already been processed
     *
     * @throws Exception
     */
    void complete() throws Exception;
}
