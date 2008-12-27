package com.goodworkalan.strata;


final class EmptyTierWriterBuilder
implements TierWriterBuilder
{
    public <B, T, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, A, X> build)
    {
        return new EmptyTierCache<B, A, X>();
    }
}