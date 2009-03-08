package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface Query<T>
{
    /**
     * Get the lock that locks the b+tree exclusively for insert and update.
     * 
     * @return The insert delete lock.
     */
    public Lock getInsertDeleteLock();
    
    
    /**
     * Get type-safe container of out of band data.
     * 
     * @return The type-safe container of out of band data.
     */
    public Stash getStash();
    
    /**
     * Get the b+tree.
     * 
     * @return The b+tree.
     */
    public Strata<T> getStrata();

    /**
     * Add the given object to the b+tree.
     * 
     * @param object
     *            The object to add.
     */
    public void add(T object);
    
    // TODO Document.
    public Comparable<? super T> newComparable(T object);

    // TODO Rename find fields? (Erasure)
    // TODO Document.
    public Cursor<T> find(Comparable<? super T> comparable);
    
    // TODO Document.
    public T remove(Deletable<T> deletable, Comparable<? super T> comparable);
    
    /**
     * Remove the first object whose index fields are equal to the given
     * comparable.
     * 
     * @param comparable
     *            The fields to match.
     * @return The removed object or null if no object is equal to the given
     *         comparable.
     */
    public T remove(Comparable<? super T> comparable);

    /**
     * Returns a cursor that references the first object in the B-Tree.
     * 
     * @return A cursor that references the first object in the B-Tree.
     */
    public Cursor<T> first();
    
    // TODO Document.
    public void flush();

    /**
     * Destroy the <code>Strata</code> B-Tree by deallocating all of its pages
     * including the root page.
     */
    public void destroy();
}
