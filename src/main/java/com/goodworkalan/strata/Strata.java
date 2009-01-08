package com.goodworkalan.strata;

// FIXME Add field to strata.
public interface Strata<T, X>
{
    public Transaction<T, X> query(X txn);
}