package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public final class BucketComparable<T, F extends Comparable<F>, B>
implements Comparable<B>
{
    private final Stash stash;
    
    private final Cooper<T, F, B> cooper;

    private final Extractor<T, F> extractor;

    private final F fields;
    
    public BucketComparable(Stash stash, Cooper<T, F, B> cooper,
                            Extractor<T, F> extractor, F fields)
    {
        this.stash = stash;
        this.cooper = cooper;
        this.extractor = extractor;
        this.fields = fields;
    }

    public int compareTo(B bucket)
    {
        return fields.compareTo(cooper.getFields(stash, extractor, bucket));
    }
}