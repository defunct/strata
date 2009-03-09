package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public class StorageAllocator<T, A> implements Allocator<T, A>
{
    // TODO Document.
    private final Storage<T, A> storage;
    
    // TODO Document.
    public StorageAllocator(Storage<T, A> storage)
    {
        this.storage = storage;
    }

    // TODO Document.
    public A allocate(Stash stash, InnerTier<T, A> inner, int capacity)
    {
        return storage.getInnerStore().allocate(stash, capacity);
    }

    // TODO Document.
    public A allocate(Stash stash, LeafTier<T, A> leaf, int capacity)
    {
        return storage.getLeafStore().allocate(stash, capacity);
    }
}
