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
     * Get the <code>Strata</code> that this query searches and edits.
     * 
     * @return The <code>Strata</code>.
     */
    public Strata<T> getStrata();

    /**
     * Add the given object to the <code>Strata</code> B+Tree.
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
     * Constructs an instance of <code>Deletable</code> whose
     * {@link Deletable#deletable(Object) deletable} method will always return
     * true. This deletable is used to remove the first stored value whose
     * fields match the comparable passed to <code>remove</code>.
     * 
     * @return An instance of <code>Deletable</code> whose
     *         <code>deletable</code> method always returns true.
     */
    public Deletable<T> deleteAny();

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
