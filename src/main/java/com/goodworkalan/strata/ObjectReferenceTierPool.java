package com.goodworkalan.strata;

final class ObjectReferenceTierPool<B, A, X>
implements TierPool<B, A, X>
{
    @SuppressWarnings("unchecked")
    public InnerTier<B, A> getInnerTier(X txn, A address)
    {
        return (InnerTier<B, A>) address;
    }
    
    @SuppressWarnings("unchecked")
    public LeafTier<B, A> getLeafTier(X txn, A address)
    {
        return (LeafTier<B, A>) address;
    }
}