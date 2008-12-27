package com.goodworkalan.strata;


public interface StorageBuilder<T, X>
{
    public Transaction<T, X> newTransaction(X txn, Schema<T, X> schema);
}