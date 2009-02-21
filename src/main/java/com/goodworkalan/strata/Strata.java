package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// FIXME Add field to strata.
public interface Strata<T, F extends Comparable<? super F>>
{
    /**
     * Create a query of the strata using the given <code>stash</code> to
     * communicate any additional participants in the storage strategy.
     * 
     * @param stash
     *            Additional participants in the storage strategy.
     * @return A new query of the strata.
     */
    public Query<T, F> query(Stash stash);

    public Query<T, F> query();
}