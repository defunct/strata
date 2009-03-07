package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface StorageBuilder<T, F extends Comparable<? super F>>
{
    // TODO Document.
    public Query<T, F> create(Stash stash, Schema<T, F> schema);
}