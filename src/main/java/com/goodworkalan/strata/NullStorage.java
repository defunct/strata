package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * A null allocator for an in memory implementation of the b-tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class NullStorage<T>
implements Storage<T, Ilk.Pair>
{
    /** The super type token of the b-tree value type. */
    private final Ilk.Key key;

    /**
     * Create a null allocator with the given super type token of b-tree value
     * type.
     * 
     * @param key
     *            The super type token of the b-tree value type.
     */
    public NullStorage(Ilk.Key key)
    {
        this.key = key;
    }

    /**
     * Allocate an inner tier by returning an super type token reference to the
     * given inner tier.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param inner
     *            The inner tier.
     * @param capacity
     *            The number of branches in the inner tier.
     * @return A super type token reference to the inner tier.
     */
    public Ilk.Pair allocate(Stash stash, InnerTier<T, Ilk.Pair> inner, int capacity)
    {
        return new Ilk<InnerTier<T, Ilk.Pair>>(key) { }.pair(inner);
    }

    /**
     * Allocate an leaf tier by returning an super type token reference to the
     * given leaf tier.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf tier.
     * @param capacity
     *            The number of values in the leaf tier.
     * @return A super type token reference to the leaf tier.
     */
    public Ilk.Pair allocate(Stash stash, LeafTier<T, Ilk.Pair> leaf, int capacity)
    {
        return new Ilk<LeafTier<T, Ilk.Pair>>(key) { }.pair(leaf);
    }
    
    // TODO Document.
    public void load(Stash stash, Ilk.Pair address, InnerTier<T, Ilk.Pair> inner)
    {
    }
    
    // TODO Document.
    public void load(Stash stash, Ilk.Pair address, LeafTier<T, Ilk.Pair> leaf)
    {
    }

    // TODO Document.
    public void write(Stash stash, InnerTier<T, Ilk.Pair> inner)
    {
    }

    // TODO Document.
    public void write(Stash stash, LeafTier<T, Ilk.Pair> leaf)
    {
    }

    // TODO Document.
    public void free(Stash stash, InnerTier<T, Ilk.Pair> inner)
    {
    }

    /**
     * A noop implementation since the in memory leaf tier is simply garbage
     * collected.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param leaf
     *            The leaf tier.
     */
    public void free(Stash stash, LeafTier<T, Ilk.Pair> leaf)
    {
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