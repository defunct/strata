package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;

/**
 * A null persistent storage strategy for an in memory implementation of the b-tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
final class InMemoryStorage<T> implements Storage<T, Ilk.Pair>
{
    /** The noop inner tier storage strategy. */
    private final InnerStore<T, Ilk.Pair> innerStore;

    /** The noop leaf tier storage strategy. */
    private final LeafStore<T, Ilk.Pair> leafStore;

    /**
     * Create a null persistent storage strategy.
     * 
     * @param key
     *            The super type token of the b-tree value type.
     */
    public InMemoryStorage()
    {
        this.innerStore = new InMemoryInnerStore<T>();
        this.leafStore = new InMemoryLeafStore<T>();
    }


    /**
     * Get the no-op inner tier storage strategy.
     * 
     * @return The no-op inner tier storage strategy.
     */
    public InnerStore<T, Ilk.Pair> getInnerStore()
    {
        return innerStore;
    }

    /**
     * Get the no-op leaf tier storage strategy.
     * 
     * @return The no-op leaf tier storage strategy.
     */
    public LeafStore<T, Ilk.Pair> getLeafStore()
    {
        return leafStore;
    }

    /**
     * Return a null super type token reference as the null address value for
     * this allocation strategy.
     * 
     * @return The null address value.
     */
    public Ilk.Pair getNull()
    {
        return null;
    }

    /**
     * Return true if the given address is null indicating that it is the null
     * value for this allocation strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(Ilk.Pair address)
    {
        return address == null;
    }
}