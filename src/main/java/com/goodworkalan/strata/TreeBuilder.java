package com.goodworkalan.strata;


public interface TreeBuilder
{
    public <T, A, X> Transaction<T, X> newTransaction(X txn, Schema<T, X> schema, Storage<T, A, X> storage);
}