package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class ObjectReferenceTierPool<B, A>
implements TierPool<B, A>
{
    // TODO Document.
    @SuppressWarnings("unchecked")
    public InnerTier<B, A> getInnerTier(Stash stash, A address)
    {
        return (InnerTier<B, A>) address;
    }
    
    // TODO Document.
    @SuppressWarnings("unchecked")
    public LeafTier<B, A> getLeafTier(Stash stash, A address)
    {
        return (LeafTier<B, A>) address;
    }
}