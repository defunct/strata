package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;


public interface StorageBuilder<T, F extends Comparable<F>>
{
    public Query<T, F> create(Stash stash, Schema<T, F> schema);
}