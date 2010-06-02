package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * An implementation of a pool of tiers for the in memory implementation of the
 * b-tree that returns tier value in the super type token reference.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
final class ObjectReferencePool<T>
implements Pool<T, Ilk.Box>
{
    /** The super type token of the b-tree value type. */
    private final Ilk.Key key;

    /**
     * Create an object reference pool from the given super type token of the
     * b-tree value type.
     * 
     * @param key
     *            The super type token of the b-tree value type.
     */
    public ObjectReferencePool(Ilk.Key key)
    {
        this.key = key;
    }

    /**
     * Return the inner tier in the super type token reference given by address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier.
     */
    public InnerTier<T, Ilk.Box> getInnerTier(Stash stash, Ilk.Box address)
    {
        return address.cast(new Ilk<InnerTier<T, Ilk.Box>>() { }.assign(new Ilk<T>() {}, key.type));
    }
    
    /**
     * Return the leaf tier in the super type token reference given by address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the leaf tier.
     */
    public LeafTier<T, Ilk.Box> getLeafTier(Stash stash, Ilk.Box address)
    {
        return address.cast(new Ilk<LeafTier<T, Ilk.Box>>() { }.assign(new Ilk<T>() {}, key.type));
    }
}