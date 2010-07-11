package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 */
public interface Strata<T> {
    /**
     * Create a query of the b+tree using the given stash to communicate any
     * additional participants in the storage strategy.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @return A new query of the b+tree.
     */
    public Query<T> query(Stash stash);

    /**
     * Create a query of the b+tree.
     * 
     * @return A new query of the b+tree.
     */
    public Query<T> query();
}