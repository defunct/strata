package com.goodworkalan.strata;

public interface TierPool<B, A, X>
{
    public LeafTier<B, A> getLeafTier(X txn, A address);
    
    public InnerTier<B, A> getInnerTier(X txn, A address);
}