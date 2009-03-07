package com.goodworkalan.strata;


// TODO Document.
public final class PerStrataTierWriterBuilder
implements TierWriterBuilder
{
    // TODO Document.
    private final int max;
    
    // TODO Document.
    public PerStrataTierWriterBuilder(int max)
    {
        this.max = max;
    }

    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierWriter<B, A> newTierWriter(Build<B, T, F, A> build)
    {
        return new PerStrataTierWriter<B, T, F, A>(build.getStorage(), build.getCooper(), build.getSchema().getExtractor(), max);
    }
}