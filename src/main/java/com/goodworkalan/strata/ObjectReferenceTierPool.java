package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

final class ObjectReferenceTierPool<B, A>
implements TierPool<B, A>
{
    @SuppressWarnings("unchecked")
    public InnerTier<B, A> getInnerTier(Stash stash, A address)
    {
        return (InnerTier<B, A>) address;
    }
    
    @SuppressWarnings("unchecked")
    public LeafTier<B, A> getLeafTier(Stash stash, A address)
    {
        return (LeafTier<B, A>) address;
    }
}