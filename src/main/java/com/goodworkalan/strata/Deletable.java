package com.goodworkalan.strata;

/**
 * Determines if an object equal to the comparator given to a remove method
 * call on a query is actually removed. For b+trees that contain duplicate
 * index values of objects, deletable is used to determine which specific
 * object to remove.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 */
public interface Deletable<T>
{
    /**
     * Return true if the given object should be deleted form the b-tree.
     * 
     * @param object
     *            The object value.
     * @return True if the object should be deleted.
     */
    public boolean deletable(T object);
}
