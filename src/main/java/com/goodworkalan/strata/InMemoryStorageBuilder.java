package com.goodworkalan.strata;


public final class InMemoryStorageBuilder<T, F extends Comparable<F>, X>
implements StorageBuilder<T, F, X>
{
    public Transaction<T, F, X> newTransaction(X txn, Schema<T, F, X> schema)
    {
        return schema.newTransaction(txn, (Storage<T, F, Object, X>) null);
    }
}