package com.goodworkalan.strata.file;

import java.nio.ByteBuffer;

/**
 * Write a record to a reference tier.
 *
 * @author Alan Gutierrez
 */
public class RecordIO implements ReferenceTierIO<Record> {
    /**
     * Read a record from storage.
     * 
     * @param bytes
     *            The byte buffer.
     * @param offset
     *            The record offset.
     * @return The record read.
     */
    public Record read(ByteBuffer bytes, int offset) {
        Record record = new Record();
        record.id = bytes.getLong(offset);
        record.version = bytes.getLong(offset + Long.SIZE / Byte.SIZE);
        return record;
    }
    
    /**
     * Write a record to storage.
     * 
     * @param bytes
     *            The byte buffer.
     * @param offset
     *            The record offset.
     * @param record
     *            The record to write.
     */
    public void write(ByteBuffer bytes, int offset, Record record) {
        bytes.putLong(offset, record.id);
        bytes.putLong(offset + Long.SIZE / Byte.SIZE, record.id);
    }

    /**
     * Get the size of the stored record in bytes.
     */
    public int getSize() {
        return Long.SIZE / Byte.SIZE * 2;
    }
}
