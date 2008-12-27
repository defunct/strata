package com.goodworkalan.strata;


public final class PerQueryTierWriterBuilder
implements TierWriterBuilder
{
    private final int max;
    
    public PerQueryTierWriterBuilder(int max)
    {
        this.max = max;
    }
    
    public <B, T, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, A, X> build)
    {
        return new PerQueryTierCache<B, T, A, X>(build.getStorage(), build.getCooper(), build.getSchema()
        .getExtractor(), max);
    }
}