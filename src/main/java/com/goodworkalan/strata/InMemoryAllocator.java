package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * A null allocator for an in memory implementation of the b-tree.
 * <p>
 * The null allocator returns an {@link Ilk.Box} that holds a reference to a
 * tier along with a super type token used to perform checked casts of the tier
 * when it is dereferenced by an {@link ObjectReferencePool}.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class InMemoryAllocator<T> implements Allocator<T, Ilk.Box>
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
    public InMemoryAllocator(Ilk.Key key)
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
    public Ilk.Box allocate(Stash stash, InnerTier<T, Ilk.Box> inner, int capacity)
    {
        return new Ilk<InnerTier<T, Ilk.Box>>(key) { }.box(inner);
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
    public Ilk.Box allocate(Stash stash, LeafTier<T, Ilk.Box> leaf, int capacity)
    {
        return new Ilk<LeafTier<T, Ilk.Box>>(key) { }.box(leaf);
    }
}
