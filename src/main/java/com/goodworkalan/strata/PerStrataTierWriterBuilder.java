package com.goodworkalan.strata;


public final class PerStrataTierWriterBuilder
implements TierWriterBuilder
{
    private final int max;
    
    public PerStrataTierWriterBuilder(int max)
    {
        this.max = max;
    }

    public <B, T, F extends Comparable<F>, A, X> TierWriter<B, A, X> newTierWriter(Build<B, T, F, A, X> build)
    {
        return new PerStrataTierWriter<B, T, F, A, X>(build.getStorage(), build.getCooper(), build.getSchema().getExtractor(), max);
    }
}