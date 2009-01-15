package com.goodworkalan.strata;


public final class PerQueryTierWriterBuilder
implements TierWriterBuilder
{
    private final int max;
    
    public PerQueryTierWriterBuilder(int max)
    {
        this.max = max;
    }
    
    public <B, T, F extends Comparable<? super F>, A> TierWriter<B, A> newTierWriter(Build<B, T, F, A> build)
    {
        return new PerQueryTierCache<B, T, F, A>(build.getStorage(), build.getCooper(), build.getSchema()
        .getExtractor(), max);
    }
}