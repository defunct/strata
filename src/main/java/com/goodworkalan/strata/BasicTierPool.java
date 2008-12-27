package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;

final class BasicTierPool<T, A, X, B>
implements TierPool<B, A, X>
{
    private final ReferenceQueue<InnerTier<B, A>> innerQueue = null;
    
    private final ReferenceQueue<LeafTier<B, A>> leafQueue = null;
    
    private final Map<A, Reference<InnerTier<B, A>>> innerTiers = null;
    
    private final Map<A, Reference<LeafTier<B, A>>> leafTiers = null;
    
    private final Storage<T, A, X> storage;
    
    private final Extractor<T, X> extractor;
    
    private final Cooper<T, B, X> cooper;
    
    public BasicTierPool(Storage<T, A, X> storage, Cooper<T, B, X> cooper, Extractor<T, X> extractor)
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
    
    public InnerTier<B, A> getInnerTier(X txn, A address)
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
                inner = storage.getInnerStore().load(txn, address, cooper, extractor);
                innerTiers.put(inner.getAddress(), new KeyedReference<A, InnerTier<B,A>>(address, inner, innerTiers, innerQueue));
            }
        }

        return inner;
    }
    
    public LeafTier<B, A> getLeafTier(X txn, A address)
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
                leaf = storage.getLeafStore().load(txn, address, cooper, extractor);
                leafTiers.put(leaf.getAddress(), new KeyedReference<A, LeafTier<B,A>>(address, leaf, leafTiers, leafQueue));
            }
        }

        return leaf;
    }
}