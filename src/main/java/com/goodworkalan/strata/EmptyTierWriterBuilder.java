package com.goodworkalan.strata;


final class EmptyTierWriterBuilder
implements TierWriterBuilder
{
    public <B, T, F extends Comparable<F>, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, F, A, X> build)
    {
        return new EmptyTierCache<B, A, X>();
    }
}