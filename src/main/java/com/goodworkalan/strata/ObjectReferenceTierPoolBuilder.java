package com.goodworkalan.strata;


final class ObjectReferenceTierPoolBuilder
implements TierPoolBuilder
{
    public <B, T, F extends Comparable<F>, A, X> TierPool<B, A, X> newTierPool(Build<B, T, F, A, X> build)
    {
        return new ObjectReferenceTierPool<B, A, X>();
    }
}