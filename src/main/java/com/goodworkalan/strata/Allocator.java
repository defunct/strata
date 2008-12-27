package com.goodworkalan.strata;

public interface Allocator<B, A, X>
{
    public A allocate(X txn, InnerTier<B, A> inner, int size);
    
    public A allocate(X txn, LeafTier<B, A> leaf, int size);
    
    public boolean isNull(A address);
    
    public A getNull();
}