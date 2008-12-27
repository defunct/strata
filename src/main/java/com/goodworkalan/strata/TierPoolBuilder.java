package com.goodworkalan.strata;


public interface TierPoolBuilder
{
    public <B, T, A, X> TierPool<B, A, X> newTierPool(Build<B, T, A, X> build);
}