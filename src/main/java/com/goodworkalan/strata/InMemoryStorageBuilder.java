package com.goodworkalan.strata;


public final class InMemoryStorageBuilder<T, X>
implements StorageBuilder<T, X>
{
    public Transaction<T, X> newTransaction(X txn, Schema<T, X> schema)
    {
        return schema.newTransaction(txn, (Storage<T, Object, X>) null);
    }
}