package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface TierPool<B, A>
{
    // TODO Document.
    public LeafTier<B, A> getLeafTier(Stash stash, A address);
    
    // TODO Document.
    public InnerTier<B, A> getInnerTier(Stash stash, A address);
}