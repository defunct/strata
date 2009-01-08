package com.goodworkalan.strata;

public final class BucketComparable<T, B, X>
implements Comparable<B>
{
    private final X txn;
    
    private final Cooper<T, B, X> cooper;

    private final Extractor<T, X> extractor;

    private final Comparable<?>[] fields;
    
    public BucketComparable(X txn, Cooper<T, B, X> cooper,
                            Extractor<T, X> extractor, Comparable<?>[] fields)
    {
        this.txn = txn;
        this.cooper = cooper;
        this.extractor = extractor;
        this.fields = fields;
    }

    public int compareTo(B bucket)
    {
        return Stratas.compare(fields, cooper.getFields(txn, extractor, bucket));
    }
}