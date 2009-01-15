package com.goodworkalan.strata;


public interface TierWriterBuilder
{
    public <B, T, F extends Comparable<? super F>, A> TierWriter<B, A> newTierWriter(Build<B, T, F, A> build);
}