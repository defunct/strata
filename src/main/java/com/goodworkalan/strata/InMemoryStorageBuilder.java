package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;


public final class InMemoryStorageBuilder<T, F extends Comparable<F>>
implements StorageBuilder<T, F>
{
    public Query<T, F> newTransaction(Stash stash, Schema<T, F> schema)
    {
        return schema.newTransaction(stash, (Storage<T, F, Object>) null);
    }
}