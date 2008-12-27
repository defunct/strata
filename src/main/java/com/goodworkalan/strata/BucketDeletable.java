package com.goodworkalan.strata;

public final class BucketDeletable<T, B, X>
implements Deletable<B>
{
    private final Cooper<T, B, X> cooper;
    
    private final Deletable<T> deletable;
    
    public BucketDeletable(Cooper<T, B, X> cooper, Deletable<T> deletable)
    {
        this.cooper = cooper;
        this.deletable = deletable;
    }
    
    public boolean deletable(B bucket)
    {
        return deletable.deletable(cooper.getObject(bucket));
    }
}