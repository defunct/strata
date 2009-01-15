package com.goodworkalan.strata;


public interface TierPoolBuilder
{
    public <B, T, F extends Comparable<? super F>, A> TierPool<B, A> newTierPool(Build<B, T, F, A> build);
}