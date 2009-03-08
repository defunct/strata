package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// FIXME Add field to strata.
// TODO Document.
public interface Strata<T>
{
    /**
     * Create a query of the strata using the given <code>stash</code> to
     * communicate any additional participants in the storage strategy.
     * 
     * @param stash
     *            Additional participants in the storage strategy.
     * @return A new query of the strata.
     */
    public Query<T> query(Stash stash);

    // TODO Document.
    public Query<T> query();
}