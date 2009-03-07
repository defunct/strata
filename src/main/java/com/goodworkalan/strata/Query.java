package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface Query<T, F extends Comparable<? super F>>
{
    /**
     * Get the stash used to store query state.
     * 
     * @return The stash.
     */
    public Stash getStash();
    
    /**
     * Get the <code>Strata</code> that this query searches and edits.
     * 
     * @return The <code>Strata</code>.
     */
    public Strata<T, F> getStrata();

    /**
     * Add the given object to the <code>Strata</code> B+Tree.
     * 
     * @param object
     *            The object to add.
     */
    public void add(T object);
    
    // TODO Rename find fields? (Erasure)
    public Cursor<T> find(Comparable<? super F> comparable);
    
    public T remove(Deletable<T> deletable, Comparable<? super F> comparable);

    /**
     * Remove the first object whose index fields are equal to the given
     * comparable.
     * 
     * @param comparable
     *            The fields to match.
     * @return The removed object or null if no object is equal to the given
     *         comparable.
     */
    public T remove(Comparable<? super F> comparable);

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

    /**
     * Extracts the fields used to index the object from the given object.
     * 
     * @param object
     *            The object to extract from.
     * @return The fields used to index the object.
     */
    public F extract(T object);
    
    /**
     * Flush the query to its persistent storage.
     */
    public void flush();

    /**
     * Destroy the <code>Strata</code> B-Tree by deallocating all of its pages
     * including the root page.
     */
    public void destroy();
}
