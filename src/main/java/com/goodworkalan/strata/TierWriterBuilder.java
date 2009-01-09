package com.goodworkalan.strata;


public interface TierWriterBuilder
{
    public <B, T, F extends Comparable<F>, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, F, A, X> build);
}