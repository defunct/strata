package com.goodworkalan.strata;

final class StorageAllocator<T, B, A, X>
implements Allocator<B, A, X>
{
    private Storage<T, A, X> storage;
    
    public StorageAllocator(Storage<T, A, X> storage)
    {
        this.storage = storage;
    }
    
    public A allocate(X txn, InnerTier<B,A> inner, int size)
    {
        return storage.getInnerStore().allocate(txn, size);
    }
    
    public A allocate(X txn, LeafTier<B,A> leaf, int size)
    {
        return storage.getLeafStore().allocate(txn, size);
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