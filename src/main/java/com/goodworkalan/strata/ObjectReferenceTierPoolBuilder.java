package com.goodworkalan.strata;


final class ObjectReferenceTierPoolBuilder
implements TierPoolBuilder
{
    public <B, T, A, X> TierPool<B, A, X> newTierPool(Build<B, T, A, X> build)
    {
        return new ObjectReferenceTierPool<B, A, X>();
    }
}