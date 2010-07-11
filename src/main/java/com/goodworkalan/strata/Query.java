package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;

import com.goodworkalan.stash.Stash;

/**
 * A query on the b-tree. A query represents group of related search, insert or
 * delete operations. When working with a persistent storage solution, a query
 * would be associated with a persistent storage transaction, so that all of the
 * operations could be recorded by a single transaction. A query is associated
 * with a specific {@link Stash} and therefore, with a specific set of
 * persistent storage transaction data.
 * <p>
 * Additionally, a thread can lock a b-tree for insert and delete exclusively,
 * either explicitly, by calling the lock method of the
 * {@link #getInsertDeleteLock()}, or implicitly, by creating the b+tree with a
 * non-zero value for {@link Schema#setMaxDirtyTiers(int)}.
 * <p>
 * NOTE: Caching dirty tiers requires an exclusive lock, so that no other thread
 * attempts to flush the dirty tiers, but I can see how the cache could be
 * common among threads, that the dirty tiers could build up, be shared among
 * threads, using to the tier level locking to control concurrency.
 * 
 * @author Alan Gutierrez
 * 
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Query<T> {
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

    /**
     * Build a comparable from the given value object using the comparable
     * factory property that provides the comparables used to order the b+tree.
     * 
     * @return A comparable built from the given object according to the order
     *         of the b+tree.
     */
    public Comparable<? super T> comparable(T object);

    /**
     * Return a forward cursor that references if the first object value in the
     * b+tree that is less than or equal to the given comparable.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return A forward cursor that references the first object value in the
     *         b+tree that is less than or equal to the given comparable.
     */
    public Cursor<T> find(Comparable<? super T> comparable);

    /**
     * Remove the first object that is equal to the given comparable that is
     * deletable according to the the given deletable if any.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return The removed object or null if no object is both equal to the
     *         given comparable and deletable according to the given deletable.
     */
    public T remove(Deletable<T> deletable, Comparable<? super T> comparable);

    /**
     * Remove the first object that is equal to the given comparable if any.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return The removed object or null if no object is equal to the given
     *         comparable.
     */
    public T remove(Comparable<? super T> comparable);

    /**
     * Returns a cursor that references the first object in the b-tree.
     * 
     * @return A cursor that references the first object in the b-tree.
     */
    public Cursor<T> first();

    /**
     * Flush any dirty tiers held by in memory and guarded by the insert and
     * delete lock.
     */
    public void flush();

    /**
     * Destroy the b+tree by deallocating all of its pages from the persistent
     * storage including the root page.
     */
    public void destroy();
}
