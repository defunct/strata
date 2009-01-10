package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;


public interface TreeBuilder
{
    public <T, F extends Comparable<F>, A> Query<T, F> newTransaction(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage);
}