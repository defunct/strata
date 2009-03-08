package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;

// TODO Document.
class InMemoryStorage<T> implements Storage<T, Ilk.Pair>
{
    // TODO Document.
    private final Ilk.Key key;
    
    // TODO Document.
    public InMemoryStorage(Ilk<T> ilk)
    {
        this.key = ilk.key;
    }

    // TODO Document.
    public Allocator<T, Ilk.Pair> getAllocator()
    {
        return new NullAllocator<T>(key);
    }
    
    // TODO Document.
    public TierPool<T, Ilk.Pair> getTierPool()
    {
        return new ObjectReferenceTierPool<T>(key);
    }
}
