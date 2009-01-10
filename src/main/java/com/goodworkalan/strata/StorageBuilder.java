package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;


public interface StorageBuilder<T, F extends Comparable<F>>
{
    public Query<T, F> newTransaction(Stash stash, Schema<T, F> schema);
}