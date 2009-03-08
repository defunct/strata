package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * Extract a comparable value from a b+tree value object. This interface is used
 * with {@link ExtractorComparableFactory} to simplify the creation of b+tree
 * indexes that are ordered by a subset of object properties.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The type of a b+tree value.
 * @param <F>
 *            A comparable type derived from the b+tree value type.
 */
public interface Extractor<T, F extends Comparable<? super F>>
{
    /**
     * Extract a comparable value from the given b+tree value object.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param object
     *            A b+tree value object.
     * @return A comparable value from the given b+tree value object.
     */
    public F extract(Stash stash, T object);
}
