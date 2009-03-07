package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class BucketComparable<T, F extends Comparable<? super F>, B>
implements Comparable<B>
{
    // TODO Document.
    private final Stash stash;
    
    // TODO Document.
    private final Cooper<T, F, B> cooper;

    // TODO Document.
    private final Extractor<T, F> extractor;

    // TODO Document.
    private final Comparable<? super F> fields;
    
    // TODO Document.
    public BucketComparable(Stash stash, Cooper<T, F, B> cooper, Extractor<T, F> extractor, Comparable<? super F> fields)
    {
        this.stash = stash;
        this.cooper = cooper;
        this.extractor = extractor;
        this.fields = fields;
    }

    // TODO Document.
    public int compareTo(B bucket)
    {
        return fields.compareTo(cooper.getFields(stash, extractor, bucket));
    }
}