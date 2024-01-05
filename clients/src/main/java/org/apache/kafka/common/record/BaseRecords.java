package org.apache.kafka.common.record;

/**
 * Base interface for accessing records which could be contained in the log, or an in-memory materialization of log records.
 */
public interface BaseRecords {
    /**
     * The size of these records in bytes.
     *
     * @return The size in bytes of the records
     */
    int sizeInBytes();

    /**
     * Encapsulate this {@link BaseRecords} object into {@link RecordsSend}
     *
     * @return Initialized {@link RecordsSend} object
     */
    RecordsSend<? extends BaseRecords> toSend();
}
