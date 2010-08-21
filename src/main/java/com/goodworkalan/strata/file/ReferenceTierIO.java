package com.goodworkalan.strata.file;

import java.nio.ByteBuffer;

/**
 * Read and write reference records
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The reference record type.
 */
public interface ReferenceTierIO<T> {
    /**
     * Read an object from the byte buffer at the given offset.
     * 
     * @param bytes
     *            The byte buffer.
     * @param offset
     *            The offset.
     * @return An record instance.
     */
    public T read(ByteBuffer bytes, int offset);

    /**
     * Write an record to the byte buffer at the given offset.
     * 
     * @param bytes
     *            The byte buffer.
     * @param offset
     *            The offset.
     * @param object
     *            The record.
     */
    public void write(ByteBuffer bytes, int offset, T object);
    
    /**
     * Get the record size.
     * 
     * @return The size of the record in bytes.
     */
    public int getSize();
}
