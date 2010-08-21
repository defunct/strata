package com.goodworkalan.strata.file;

import java.nio.ByteBuffer;

import com.goodworkalan.strata.Tier;
/**
 * An implementation of a <code>Tier</code> that can hold primitive Java
 * values, suitable for a tree that contains references to file positions using
 * identifiers.
 * <p>
 * The first record is occupied by a flag that indicates if it is a leaf. The
 * last record of a leaf is a reference to the next leaf tier.
 * 
 * @author Alan Gutierrez
 */
public class ReferenceTierCassette<T> extends Tier<T, Long> {
    /** The record tier I/O strategy. */
    private final ReferenceTierIO<T> io;
    
    /** The byte buffer. */
    private final ByteBuffer bytes;
    
    /** The offset into the byte buffer. */
    private final int offset;

    /**
     * Create a file tier cassette.
     * 
     * @param io
     *            The record tier I/O strategy.
     * @param bytes
     *            The byte buffer.
     * @param offset
     *            The offset.
     */
    public ReferenceTierCassette(ReferenceTierIO<T> io, ByteBuffer bytes, int offset) {
        this.io = io;
        this.bytes = bytes;
        this.offset = offset;
    }

    /**
     * Get whether this tier is a leaf.
     * 
     * @return True if this tier is a leaf tier.
     */
    public boolean isChildLeaf() {
        return bytes.get(getHeaderOffset()) == 1;
    }
    
    /**
     * Set whether this tier is a leaf.
     * 
     * @param leaf True if this tier is a leaf tier.
     */
    public void setChildLeaf(boolean leaf) {
        bytes.put(getHeaderOffset(), (byte) (leaf  ? 1 : 0));
    }

    /**
     * Read a record from the tier at the given index.
     * 
     * @param index
     *            The record index.
     * @return A record.
     */
    public T getRecord(int index) {
        return io.read(bytes, getOffset(index));
    }
    
    /**
     * Write an record to the tier at the given offset.
     * 
     * @param offset
     *            The offset.
     * @param object
     *            The record.
     */
    public void setRecord(int index, T record) {
        io.write(bytes, offset, record);
    }

    /**
     * Get the offset of the tier header.
     * 
     * @return The offset of the header.
     */
    private int getHeaderOffset() {
        return offset;
    }

    /**
     * Get the record offset for the record at the given index.
     * 
     * @param index
     *            The record index.
     * @return The record offset for the record at the given index.
     */
    public int getOffset(int index) {
        return io.getSize() * index;
    }
}
