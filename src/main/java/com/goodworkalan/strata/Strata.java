package com.goodworkalan.strata;

// FIXME Add field to strata.
public interface Strata<T, F extends Comparable<F>, X>
{
    public Transaction<T, F, X> query(X txn);
}