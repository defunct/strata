package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A storage strategy for allocation and deallocation of tier storage.
 * 
 * @author Alan Gutierrez
 * 
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Store<A> {
    /**
     * Allocate persistent storage that can hold the given number of objects.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param capacity
     *            The number of objects to store.
     * @return The address of the persistent storage.
     */
    public A allocate(Stash stash, int capacity);

    /**
     * Free the persistent storage at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address to free.
     */
    public void free(Stash stash, A address);
}
