package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;

import com.goodworkalan.stash.Stash;

final class BasicTierPool<T, F extends Comparable<? super F>, A, B>
implements TierPool<B, A>
{
    private final ReferenceQueue<InnerTier<B, A>> innerQueue = null;
    
    private final ReferenceQueue<LeafTier<B, A>> leafQueue = null;
    
    private final Map<A, Reference<InnerTier<B, A>>> innerTiers = null;
    
    private final Map<A, Reference<LeafTier<B, A>>> leafTiers = null;
    
    private final Storage<T, F, A> storage;
    
    private final Extractor<T, F> extractor;
    
    private final Cooper<T, F, B> cooper;
    
    public BasicTierPool(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor)
    {
        this.storage = storage;
        this.cooper = cooper;
        this.extractor = extractor;
    }
    
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
    
    public InnerTier<B, A> getInnerTier(Stash stash, A address)
    {
        collect();
        
        InnerTier<B, A> inner = null;
        
        synchronized (innerTiers)
        {
            Reference<InnerTier<B, A>> reference = innerTiers.get(address);
            if (reference != null)
            {
                inner = reference.get();
            }
            if (inner == null)
            {
                inner = storage.getInnerStore().load(stash, address, cooper, extractor);
                innerTiers.put(inner.getAddress(), new KeyedReference<A, InnerTier<B,A>>(address, inner, innerTiers, innerQueue));
            }
        }

        return inner;
    }
    
    public LeafTier<B, A> getLeafTier(Stash stash, A address)
    {
        collect();

        LeafTier<B, A> leaf = null;
        
        synchronized (leafTiers)
        {
            Reference<LeafTier<B, A>> reference = leafTiers.get(address);
            if (reference != null)
            {
                leaf = reference.get();
            }
            if (leaf == null)
            {
                leaf = storage.getLeafStore().load(stash, address, cooper, extractor);
                leafTiers.put(leaf.getAddress(), new KeyedReference<A, LeafTier<B,A>>(address, leaf, leafTiers, leafQueue));
            }
        }

        return leaf;
    }
}