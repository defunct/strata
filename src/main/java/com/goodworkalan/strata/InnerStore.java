package com.goodworkalan.strata;

import java.util.Collection;

import com.goodworkalan.stash.Stash;

/**
 * A storage strategy to read and write inner tiers.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface InnerStore<T, A> extends Store<A> {
    /**
     * Load a collection of inner tier branches from the persistent storage at
     * the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param objects
     *            The collection to load.
     * @return The child type of the loaded collection or null if the collection
     *         is a leaf.
     */
    public ChildType load(Stash stash, A address, Collection<Branch<T, A>> objects);
    
    /**
     * Write a collection of inner tier branches to the persistent storage at
     * the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param branches
     *            The branches to write.
     * @param childType
     *            The child type.
     */
    public void write(Stash stash, A address, Collection<Branch<T, A>> branches, ChildType childType);
}
