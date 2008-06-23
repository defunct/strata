/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class BucketComparable<T, F extends Comparable<? super F>, B, X>
implements Comparable<B>
{
    private Cooper<T, F, B, X> cooper;
    
    private Extractor<T, F, X> extractor;
    
    private X txn;
    
    private F fields;
    
    public int compareTo(B bucket)
    {
        F toFields = cooper.getFields(txn, extractor, bucket);
        return fields.compareTo(toFields);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */