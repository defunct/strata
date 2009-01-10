package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public interface Allocator<B, A>
{
    public A allocate(Stash stash, InnerTier<B, A> inner, int size);
    
    public A allocate(Stash stash, LeafTier<B, A> leaf, int size);
    
    public boolean isNull(A address);
    
    public A getNull();
}