package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A comparable factory that creates a comparable by casting the b+tree
 * value object to a comprable.
 *
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
public class CastComparableFactory<T extends Comparable<? super T>>
implements ComparableFactory<T>
{
    /**
     * Create a comparable based on the given objects that will compare the
     * given object to objects of the same type.
     * 
     * @return A comparable based on the given objects that will compare the
     *         given object to objects of the same type.
     */
    public Comparable<? super T> newComparable(Stash stash, T object)
    {
        return object;
    }
}
