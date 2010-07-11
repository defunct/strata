package com.goodworkalan.strata;

import java.util.Collection;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * A null persistent storage strategy to read and write inner tiers for an in
 * memory implementation of the b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class InMemoryInnerStore<T>
extends InMemoryStore
implements InnerStore<T, Ilk.Box> {
    /**
     * Throws an exception because the object reference pool will never call the
     * null store load method.
     * 
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param objects
     *            The collection to load.
     * @return The child type of the loaded collection or null if the collection
     *         is a leaf.
     *              @exception UnsupportedOperationException
     *                Since this method should never be called.
     */
    public ChildType load(Stash stash, Ilk.Box address, Collection<Branch<T, Ilk.Box>> objects) {
        throw new UnsupportedOperationException();
    }
    

    /**
     * Write a collection of objects to the persistent storage at the given
     * address.
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
    public void write(Stash stash, Ilk.Box address, Collection<Branch<T, Ilk.Box>> objects, ChildType childType) {
    }
}
