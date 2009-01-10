package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

final class StorageAllocator<T, F extends Comparable<F>, B, A>
implements Allocator<B, A>
{
    private Storage<T, F, A> storage;
    
    public StorageAllocator(Storage<T, F, A> storage)
    {
        this.storage = storage;
    }
    
    public A allocate(Stash stash, InnerTier<B,A> inner, int size)
    {
        return storage.getInnerStore().allocate(stash, size);
    }
    
    public A allocate(Stash stash, LeafTier<B,A> leaf, int size)
    {
        return storage.getLeafStore().allocate(stash, size);
    }
    
    public A getNull()
    {
        return storage.getNull();
    }
    
    public boolean isNull(A address)
    {
        return storage.isNull(address);
    }
}