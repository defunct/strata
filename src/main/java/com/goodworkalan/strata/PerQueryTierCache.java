package com.goodworkalan.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.stash.Stash;

/**
 * A tier cache that maintains a map of dirty tiers per query and that prevents
 * other queries from inserting or deleting items until the tier cache is
 * emptied and dirty tiers are persisted.
 * <p>
 * The exclusive tree lock will be called when an insert or delete begins and
 * the map of dirty tiers is empty. At the end of an insert or delete, if the
 * map of dirty tiers is empty, the exclusive tree lock is released. Thus, the
 * empty map of dirty tiers is an indicator that the associated query does not
 * hold the lock on the tree.
 * <p>
 * FIXME Rename PerQueryTeirWriter.
 */
public class PerQueryTierCache<B, T, F extends Comparable<? super F>, A>
extends AbstractTierCache<B, T, F, A>
{
    /**
     * Create a per query tier writer.
     * 
     * @param lock
     *           An exclusive lock to guard inserts and deletes on the tree.
     * @param max
     *            The dirty tier writer size that when reached, will cause the
     *            cache to empty and the tiers to be written.
     */
    public PerQueryTierCache(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, int max)
    {
        this(storage, cooper, extractor, new ReentrantLock(), max, true);
    }

    /**
     * Create a copy of a per query tier writer.
     * 
     * @param storage
     *            The persistent storage strategy.
     * @param cooper
     *            The cooper to use to create a bucket to store the index
     *            fields.
     * @param extractor
     *            The extractor to use to extract the index fields.
     * @param lock
     *            An exclusive lock to guard inserts and deletes on the tree.
     * @param max
     *            The dirty tier cache size that when reached, will cause the
     *            cache to empty and the tiers to be written.
     * @param autoCommit
     *            If true, the commit method of the storage strategy is called
     *            after the dirty tiers are written.
     */
    private PerQueryTierCache(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, Lock lock, int max, boolean autoCommit)
    {
        super(storage, cooper, extractor,
              lock,
              new Object(),
              max,
              autoCommit);
    }

    /**
     * Notify the per query tier writer that an insert or delete is about to
     * begin so that the per query tier writer can acquire an exclusive
     * write lock on the tree.
     */
    public void begin()
    {
        if (size() == 0)
        {
            lock();
        }
    }

    /**
     * Notify the per query tier writer that an insert or delete has completed
     * so that the per query tier writer can flush the dirty pages and release
     * the exclusive write lock on the tree if the threshold for in memory pages
     * has been reached.
     */
    public void end(Stash stash)
    {
        save(stash, false);
        if (size() == 0)
        {
            unlock();
        }
    }

    /**
     * Returns a new per query tier writer built from this prototype per query
     * tier writer. This will be a per query empty tier writer with the same max
     * and auto commit properties, that references the same storage, cooper,
     * executor, and exclusive lock on the tree.
     * 
     * @return A new tier writer based on this prototype instance.
     */
    public TierWriter<B, A> newTierWriter()
    {
        return new PerQueryTierCache<B, T, F, A>(getStorage(), cooper, extractor, lock, max, isAutoCommit());
    }
}