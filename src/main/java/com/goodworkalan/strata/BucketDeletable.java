package com.goodworkalan.strata;

public final class BucketDeletable<T, F extends Comparable<? super F>, B>
implements Deletable<B>
{
    private final Cooper<T, F, B> cooper;
    
    private final Deletable<T> deletable;
    
    public BucketDeletable(Cooper<T, F, B> cooper, Deletable<T> deletable)
    {
        this.cooper = cooper;
        this.deletable = deletable;
    }
    
    public boolean deletable(B bucket)
    {
        return deletable.deletable(cooper.getObject(bucket));
    }
}