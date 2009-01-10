package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

final class NullAllocator<B, A>
implements Allocator<B, A>
{
    @SuppressWarnings("unchecked")
    public A allocate(Stash stash, InnerTier<B, A> inner, int size)
    {
        return (A) inner;
    }
    
    @SuppressWarnings("unchecked")
    public A allocate(Stash stash, LeafTier<B, A> leaf, int size)
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