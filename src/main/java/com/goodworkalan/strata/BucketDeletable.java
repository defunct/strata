package com.goodworkalan.strata;

public final class BucketDeletable<T, F extends Comparable<F>, B, X>
implements Deletable<B>
{
    private final Cooper<T, F, B, X> cooper;
    
    private final Deletable<T> deletable;
    
    public BucketDeletable(Cooper<T, F, B, X> cooper, Deletable<T> deletable)
    {
        this.cooper = cooper;
        this.deletable = deletable;
    }
    
    public boolean deletable(B bucket)
    {
        return deletable.deletable(cooper.getObject(bucket));
    }
}