package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

// FIXME Add field to strata.
public interface Strata<T, F extends Comparable<F>>
{
    public Query<T, F> query(Stash stash);

    public Query<T, F> query();
}