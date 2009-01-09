package com.goodworkalan.strata;


public final class PerQueryTierWriterBuilder
implements TierWriterBuilder
{
    private final int max;
    
    public PerQueryTierWriterBuilder(int max)
    {
        this.max = max;
    }
    
    public <B, T, F extends Comparable<F>, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, F, A, X> build)
    {
        return new PerQueryTierCache<B, T, F, A, X>(build.getStorage(), build.getCooper(), build.getSchema()
        .getExtractor(), max);
    }
}