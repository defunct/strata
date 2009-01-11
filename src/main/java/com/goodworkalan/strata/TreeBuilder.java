package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;


public interface TreeBuilder
{
    public <T, F extends Comparable<F>, A> Construction<T, F, A> create(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage);

    public <T, F extends Comparable<F>, A> Strata<T, F> open(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage, A rootAddress);
}