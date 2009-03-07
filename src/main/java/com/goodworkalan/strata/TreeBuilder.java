package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface TreeBuilder
{
    // TODO Document.
    public <T, F extends Comparable<? super F>, A> Construction<T, F, A> create(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage);

    // TODO Document.
    public <T, F extends Comparable<? super F>, A> Strata<T, F> open(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage, A rootAddress);
}