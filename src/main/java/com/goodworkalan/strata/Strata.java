package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// FIXME Add field to strata.
public interface Strata<T, F extends Comparable<? super F>>
{
    public Query<T, F> query(Stash stash);

    public Query<T, F> query();
}