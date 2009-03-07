package com.goodworkalan.strata;

// TODO Document.
public interface TierPoolBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierPool<B, A> newTierPool(Build<B, T, F, A> build);
}