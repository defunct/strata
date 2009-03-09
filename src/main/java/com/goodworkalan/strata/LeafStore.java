package com.goodworkalan.strata;

import java.util.Collection;

import com.goodworkalan.stash.Stash;

/**
 * A storage strategy to read and write leaf tiers.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface LeafStore<T, A> extends Store<A>
{
    /**
     * Load a collection of value objects from the persistent storage at the
     * given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param objects
     *            The collection to load.
     * @return The address of the next leaf in the b-tree.
     */
    public A load(Stash stash, A address, Collection<T> objects);

    /**
     * Write a collection of leaf tier value objects to the persistent storage
     * at the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier storage.
     * @param objects
     *            The collection to write.
     * @param next
     *            The address of the next leaf in the b-tree.
     */
    public void write(Stash stash, A address, Collection<T> objects, A next);
}
