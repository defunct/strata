package com.goodworkalan.strata;


//TODO Document.
public interface TierWriterBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierWriter<B, A> newTierWriter(Build<B, T, F, A> build);
}