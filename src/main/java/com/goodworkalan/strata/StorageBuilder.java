package com.goodworkalan.strata;


public interface StorageBuilder<T, F extends Comparable<F>, X>
{
    public Transaction<T, F, X> newTransaction(X txn, Schema<T, F, X> schema);
}