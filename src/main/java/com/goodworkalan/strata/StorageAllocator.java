package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class StorageAllocator<T, F extends Comparable<? super F>, B, A>
implements Allocator<B, A>
{
    // TODO Document.
    private Storage<T, F, A> storage;
    
    // TODO Document.
    public StorageAllocator(Storage<T, F, A> storage)
    {
        this.storage = storage;
    }
    
    // TODO Document.
    public A allocate(Stash stash, InnerTier<B,A> inner, int size)
    {
        return storage.getInnerStore().allocate(stash, size);
    }
    
    // TODO Document.
    public A allocate(Stash stash, LeafTier<B,A> leaf, int size)
    {
        return storage.getLeafStore().allocate(stash, size);
    }
    
    // TODO Document.
    public A getNull()
    {
        return storage.getNull();
    }
    
    // TODO Document.
    public boolean isNull(A address)
    {
        return storage.isNull(address);
    }
}