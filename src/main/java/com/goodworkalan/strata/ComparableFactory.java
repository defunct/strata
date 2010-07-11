package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A factory that creates a comparable based on an object in the b+tree to
 * compare against other objects in the b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 */
public interface ComparableFactory<T> {
    /**
     * Create a comparable based on the given objects that will compare the
     * given object to objects of the same type.
     * 
     * @return A comparable based on the given objects that will compare the
     *         given object to objects of the same type.
     */
    public Comparable<? super T> newComparable(Stash stash, T object);
}
