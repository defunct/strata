package com.goodworkalan.strata;


public interface TierPoolBuilder
{
    public <B, T, F extends Comparable<F>, A, X> TierPool<B, A, X> newTierPool(Build<B, T, F, A, X> build);
}