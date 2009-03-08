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
public final class BasicTierPool<T, A>
implements TierPool<T, A>
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
    private final Allocator<T, A> allocator;

    /**
     * Create a new basic tier pool.
     * 
     * @param allocator
     *            The allocator to use to load pages from disk.
     */
    public BasicTierPool(Allocator<T, A> allocator)
    {
        this.allocator = allocator;
    }
    
    // TODO Document.
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
    
    // TODO Document.
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
                allocator.load(stash, address, inner);
                innerTiers.put(inner.getAddress(), new KeyedReference<InnerTier<T,A>, A>(address, inner, innerTiers, innerQueue));
            }
        }

        return inner;
    }
    
    // TODO Document.
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
                allocator.load(stash, address, leaf);
                leafTiers.put(leaf.getAddress(), new KeyedReference<LeafTier<T, A>, A>(address, leaf, leafTiers, leafQueue));
            }
        }

        return leaf;
    }
}