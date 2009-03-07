package com.goodworkalan.strata;

// TODO Document.
public final class BasicTierPoolBuilder
implements TierPoolBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierPool<B, A> newTierPool(Build<B, T, F, A> build)
    {
        return new BasicTierPool<T, F, A, B>(build.getStorage(), build.getCooper(), build.getSchema().getExtractor());
    }
}