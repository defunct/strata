package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public interface TierPool<B, A>
{
    public LeafTier<B, A> getLeafTier(Stash stash, A address);
    
    public InnerTier<B, A> getInnerTier(Stash stash, A address);
}