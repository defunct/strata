package com.goodworkalan.strata;

public interface Extractor<T, F extends Comparable<F>, X>
{
    public F extract(X txn, T object);
}