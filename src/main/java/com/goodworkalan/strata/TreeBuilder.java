package com.goodworkalan.strata;


public interface TreeBuilder
{
    public <T, F extends Comparable<F>, A, X> Transaction<T, F, X> newTransaction(X txn, Schema<T, F, X> schema, Storage<T, F, A, X> storage);
}