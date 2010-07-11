package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * A null persistent storage strategy for inner and leaf tiers for an in memory
 * implementation of the b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
public class InMemoryStore implements Store<Ilk.Box> {
    /**
     * Throws an exception because the object reference pool will never call the
     * null store load method.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param capacity
     *            The number of objects to store.
     * @return The address of the persistent storage.
     * @exception UnsupportedOperationException
     *                Since this method should never be called.
     */
    public Ilk.Box allocate(Stash stash, int capacity) {
        throw new UnsupportedOperationException();
    }

    /**
     * Free the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address to free.
     */
    public void free(Stash stash, Ilk.Box address) {
    }
}
