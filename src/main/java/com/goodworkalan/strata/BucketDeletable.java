package com.goodworkalan.strata;

// TODO Document.
public final class BucketDeletable<T, F extends Comparable<? super F>, B>
implements Deletable<B>
{
    // TODO Document.
    private final Cooper<T, F, B> cooper;
    
    // TODO Document.
    private final Deletable<T> deletable;
    
    // TODO Document.
    public BucketDeletable(Cooper<T, F, B> cooper, Deletable<T> deletable)
    {
        this.cooper = cooper;
        this.deletable = deletable;
    }
    
    // TODO Document.
    public boolean deletable(B bucket)
    {
        return deletable.deletable(cooper.getObject(bucket));
    }
}