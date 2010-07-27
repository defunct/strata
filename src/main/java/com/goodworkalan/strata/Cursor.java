package com.goodworkalan.strata;

import java.util.Iterator;

/**
 * A cursor that iterates forward over the leaves of a b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <Record>
 *            The value type of the b+tree objects.
 */
public interface Cursor<Record> extends Iterator<Record> {
    /**
     * Release the cursor by releasing the read lock on the current leaf tier.
     */
    public void release();
    
    /**
     * Create a new cursor that starts from the current location of this cursor.
     * 
     * @return A new cursor based on this cursor.
     */
    public Cursor<Record> newCursor();
}