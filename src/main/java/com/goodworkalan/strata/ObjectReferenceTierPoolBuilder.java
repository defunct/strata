package com.goodworkalan.strata;


final class ObjectReferenceTierPoolBuilder
implements TierPoolBuilder
{
    public <B, T, F extends Comparable<F>, A> TierPool<B, A> newTierPool(Build<B, T, F, A> build)
    {
        return new ObjectReferenceTierPool<B, A>();
    }
}