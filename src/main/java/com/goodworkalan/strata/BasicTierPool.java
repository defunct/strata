package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class BasicTierPool<T, A>
implements TierPool<T, A>
{
    // TODO Document.
    private final ReferenceQueue<InnerTier<T, A>> innerQueue = null;
    
    // TODO Document.
    private final ReferenceQueue<LeafTier<T, A>> leafQueue = null;
    
    // TODO Document.
    private final Map<A, Reference<InnerTier<T, A>>> innerTiers = null;
    
    // TODO Document.
    private final Map<A, Reference<LeafTier<T, A>>> leafTiers = null;
    
    // TODO Document.
    private final Allocator<T, A> allocator;
    
    // TODO Document.
    public BasicTierPool(Allocator<T, A> storage)
    {
        this.allocator = storage;
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