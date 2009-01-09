package com.goodworkalan.strata;

public final class BucketComparable<T, F extends Comparable<F>, B, X>
implements Comparable<B>
{
    private final X txn;
    
    private final Cooper<T, F, B, X> cooper;

    private final Extractor<T, F, X> extractor;

    private final F fields;
    
    public BucketComparable(X txn, Cooper<T, F, B, X> cooper,
                            Extractor<T, F, X> extractor, F fields)
    {
        this.txn = txn;
        this.cooper = cooper;
        this.extractor = extractor;
        this.fields = fields;
    }

    public int compareTo(B bucket)
    {
        return fields.compareTo(cooper.getFields(txn, extractor, bucket));
    }
}