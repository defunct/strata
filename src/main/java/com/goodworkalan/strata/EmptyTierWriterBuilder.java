package com.goodworkalan.strata;

// TODO Document.
final class EmptyTierWriterBuilder
implements TierWriterBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierWriter<B, A> newTierWriter(Build<B, T, F, A> build)
    {
        return new EmptyTierCache<B, A>();
    }
}