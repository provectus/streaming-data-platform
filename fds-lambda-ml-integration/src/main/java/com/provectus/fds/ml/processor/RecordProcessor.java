package com.provectus.fds.ml.processor;

import com.amazonaws.services.athena.model.Row;

import java.util.Map;

/**
 * Base interface for record processing
 */
public interface RecordProcessor extends AutoCloseable {

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
     * Returns statistics about processed records. Same implementations
     * may have reliable statistic only after method comlete() has been
     * called
     *
     * @return Map with implementation specific values
     */
    Map<String, String> getStatistic();
}
