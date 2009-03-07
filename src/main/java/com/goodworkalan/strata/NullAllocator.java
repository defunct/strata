package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class NullAllocator<B, A>
implements Allocator<B, A>
{
    // TODO Document.
    @SuppressWarnings("unchecked")
    public A allocate(Stash stash, InnerTier<B, A> inner, int size)
    {
        return (A) inner;
    }
    
    // TODO Document.
    @SuppressWarnings("unchecked")
    public A allocate(Stash stash, LeafTier<B, A> leaf, int size)
    {
        return (A) leaf;
    }
    
    // TODO Document.
    public boolean isNull(A address)
    {
        return address == null;
    }
    
    // TODO Document.
    public A getNull()
    {
        return null;
    }
}