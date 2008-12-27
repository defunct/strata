package com.goodworkalan.strata;


public final class BasicTierPoolBuilder
implements TierPoolBuilder
{
    public <B, T, A, X> TierPool<B, A, X> newTierPool(Build<B, T, A, X> build)
    {
        return new BasicTierPool<T, A, X, B>(build.getStorage(), build.getCooper(), build.getSchema().getExtractor());
    }
}