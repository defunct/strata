package com.goodworkalan.strata;

import java.util.Collection;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

public class InMemoryLeafStore<T>
extends InMemoryStore
implements LeafStore<T, Ilk.Pair>
{
    /**
     * Throws an exception because the object reference pool will never call the
     * null store load method.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param objects
     *            The collection to load.
     * @return The address of the next leaf in the b-tree.
     * @exception UnsupportedOperationException
     *                Since this method should never be called.
     */
    public Ilk.Pair load(Stash stash, Ilk.Pair address, Collection<T> objects)
    {
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
     * @param objects
     *            The collection to write.
     * @param next
     *            The address of the next leaf in the b-tree.
     */
    public void write(Stash stash, Ilk.Pair address, Collection<T> objects, Ilk.Pair next)
    {
    }
}
