package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;

import com.goodworkalan.stash.Stash;

/**
 * A basic caching tier pool that maintains a map of softly referenced tiers
 * in memory.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class BasicPool<T, A>
implements Pool<T, A>
{
    /** A queue of references to inner tiers. */
    private final ReferenceQueue<InnerTier<T, A>> innerQueue = new ReferenceQueue<InnerTier<T,A>>();
    
    /** A queue of references to leaf tiers. */
    private final ReferenceQueue<LeafTier<T, A>> leafQueue = new ReferenceQueue<LeafTier<T,A>>();
    
    /** A map of addresses to inner tiers. */
    private final Map<A, Reference<InnerTier<T, A>>> innerTiers = new HashMap<A, Reference<InnerTier<T,A>>>();
    
    /** A map of addresses to leaf tiers. */
    private final Map<A, Reference<LeafTier<T, A>>> leafTiers = new HashMap<A, Reference<LeafTier<T,A>>>();
    
    /** The allocator to use to load pages from disk. */
    private final Storage<T, A> allocator;

    /**
     * Create a new basic tier pool.
     * 
     * @param allocator
     *            The allocator to use to load pages from disk.
     */
    public BasicPool(Storage<T, A> allocator)
    {
        this.allocator = allocator;
    }

    /**
     * Remove unreferenced inner and leaf tiers from the address to tier maps.
     */
    private void collect()
    {
        synchronized (innerTiers)
        {
            Unmappable unmappable = null;
            while ((unmappable = (Unmappable) innerQueue.poll()) != null)
            {
                unmappable.unmap();
            }
        }
        synchronized (leafTiers)
        {
            Unmappable unmappable = null;
            while ((unmappable = (Unmappable) leafQueue.poll()) != null)
            {
                unmappable.unmap();
            }
        }
    }

    /**
     * Get the inner tier for the given address. The basic pool will keep a soft
     * reference to the inner tier in an in memory cache. If the inner tier is
     * available in the in memory cache, it is returned. If it is not available,
     * it is loaded and cached.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of an inner tier.
     * @return The inner tier for the given address.
     */
    public InnerTier<T, A> getInnerTier(Stash stash, A address)
    {
        collect();
        
        InnerTier<T, A> inner = null;
        
        synchronized (innerTiers)
        {
            Reference<InnerTier<T, A>> reference = innerTiers.get(address);
            if (reference != null)
            {
                inner = reference.get();
            }
            if (inner == null)
            {
                inner = new InnerTier<T, A>();
                allocator.getInnerStore().load(stash, address, inner);
                innerTiers.put(inner.getAddress(), new KeyedReference<A, InnerTier<T,A>>(address, inner, innerTiers, innerQueue));
            }
        }

        return inner;
    }
    
    /**
     * Get the leaf tier for the given address. The basic pool will keep a soft
     * reference to the inner tier in an in memory cache. If the inner tier is
     * available in the in memory cache, it is returned. If it is not available,
     * it is loaded and cached.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of an inner tier.
     * @return The inner tier for the given address.
     */
    public LeafTier<T, A> getLeafTier(Stash stash, A address)
    {
        collect();

        LeafTier<T, A> leaf = null;
        
        synchronized (leafTiers)
        {
            Reference<LeafTier<T, A>> reference = leafTiers.get(address);
            if (reference != null)
            {
                leaf = reference.get();
            }
            if (leaf == null)
            {
                leaf = new LeafTier<T, A>();
                allocator.getLeafStore().load(stash, address, leaf);
                leafTiers.put(leaf.getAddress(), new KeyedReference<A, LeafTier<T, A>>(address, leaf, leafTiers, leafQueue));
            }
        }

        return leaf;
    }
}