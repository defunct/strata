package com.goodworkalan.strata;

final class NullAllocator<B, A, X>
implements Allocator<B, A, X>
{
    @SuppressWarnings("unchecked")
    public A allocate(X txn, InnerTier<B, A> inner, int size)
    {
        return (A) inner;
    }
    
    @SuppressWarnings("unchecked")
    public A allocate(X txn, LeafTier<B, A> leaf, int size)
    {
        return (A) leaf;
    }
    
    public boolean isNull(A address)
    {
        return address == null;
    }
    
    public A getNull()
    {
        return null;
    }
}