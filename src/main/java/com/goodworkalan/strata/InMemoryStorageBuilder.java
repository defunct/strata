package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class InMemoryStorageBuilder<T, F extends Comparable<? super F>>
implements StorageBuilder<T, F>
{
    // TODO Document.
    public Query<T, F> create(Stash stash, Schema<T, F> schema)
    {
        return schema.create(stash, (Storage<T, F, Object>) null).getQuery();
    }
}