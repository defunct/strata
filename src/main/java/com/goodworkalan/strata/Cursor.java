package com.goodworkalan.strata;

import java.util.Iterator;

/**
 * A cursor that iterates forward over the leaves of a b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
public interface Cursor<T>
extends Iterator<T>
{
    /**
     * Release the cursor by releasing the read lock on the current leaf tier.
     */
    public void release();
}