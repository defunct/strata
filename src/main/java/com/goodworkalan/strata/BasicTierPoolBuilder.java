package com.goodworkalan.strata;


public final class BasicTierPoolBuilder
implements TierPoolBuilder
{
    public <B, T, F extends Comparable<F>, A, X> TierPool<B, A, X> newTierPool(Build<B, T, F, A, X> build)
    {
        return new BasicTierPool<T, F, A, X, B>(build.getStorage(), build.getCooper(), build.getSchema().getExtractor());
    }
}