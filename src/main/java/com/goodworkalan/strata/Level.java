package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * The per level mutation state container.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class Level<T, A>
{
    /**
     * A swappable strategy that extracts either the read or write lock from a
     * read/write lock.
     */
    public LockExtractor locker;

    /**
     * A map of addresses to locked tiers that keeps track of the tiers that
     * need to be unlocked as well as holds onto a hard reference to the tiers
     * so that they are not garbage collected.
     */
    public final Map<Object, Tier<?, A>> lockedTiers = new HashMap<Object, Tier<?, A>>();

    /** The list of operations to perform on the tier at this level. */
    public final LinkedList<Operation<T, A>> operations = new LinkedList<Operation<T, A>>();

    /**
     * Create a new level that will lock tiers exclusively if the given
     * exclusive value it true.
     * 
     * @param exclusive
     *            If true, this level will lock tiers exclusively.
     */
    public Level(boolean exclusive)
    {
        this.locker = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
    }

    /**
     * Lock the given tier according to the locker property and add it to the
     * set of locked tiers. The set of locked tiers keeps track of the tiers that
     * need to be unlocked as well as holds onto a hard reference to the tiers
     * so that they are not garbage collected.
     * 
     * @param tier
     *            The tier to lock.
     */
    public void lockAndAdd(Tier<?, A> tier)
    {
        lock_(tier);
        add_(tier);
    }

    /**
     * Unlock the given tier according to the locker property and remove it from
     * the set of locked tiers. The set of locked tiers keeps track of the tiers that
     * need to be unlocked as well as holds onto a hard reference to the tiers
     * so that they are not garbage collected.
     * 
     * @param tier
     *            The tier to lock.
     */
    public void unlockAndRemove(Tier<?, A> tier)
    {
        assert lockedTiers.containsKey(tier.getAddress());
        
        lockedTiers.remove(tier.getAddress());
        unlock_(tier);
    }

    /**
     * Add the tier to the set of locked tiers. The set of locked tiers keeps
     * track of the tiers that need to be unlocked as well as holds onto a hard
     * reference to the tiers so that they are not garbage collected.
     * 
     * @param tier
     *            The tier to add to the set of locked tiers.
     */
    public void add_(Tier<?, A> tier)
    {
        lockedTiers.put(tier.getAddress(), tier);
    }

    /**
     * Lock the given tier according to the locker property
     * 
     * @param tier
     *            The tier to lock.
     */
    public void lock_(Tier<?, A> tier)
    {
        locker.getLock(tier.getReadWriteLock()).lock();
    }

    /**
     * Unlock the given tier according to the locker property
     * 
     * @param tier
     *            The tier to unlock.
     */
    public void unlock_(Tier<?, A> tier)
    {
        locker.getLock(tier.getReadWriteLock()).unlock();
    }

    /**
     * Unlock all the tiers in the set of tiers according to the locker
     * property.
     */
    public void release()
    {
        for (Tier<?, A> tier : lockedTiers.values())
        {
            locker.getLock(tier.getReadWriteLock()).unlock();
        }
    }

    /**
     * Unlock all the tiers in the set of tiers according to the locker property
     * and remove them from the set of locked tiers. The set of locked tiers
     * keeps track of the tiers that need to be unlocked as well as holds onto a
     * hard reference to the tiers so that they are not garbage collected.
     */
    public void releaseAndClear()
    {
        for (Tier<?, A> tier : lockedTiers.values())
        {
            locker.getLock(tier.getReadWriteLock()).unlock();
        }
        lockedTiers.clear();
    }

    // TODO Document.
    private void exclusive()
    {
        for (Tier<?, A> tier : lockedTiers.values())
        {
            tier.getReadWriteLock().writeLock().lock();
        }
        locker = new WriteLockExtractor();
    }

    // TODO Document.
    public void downgrade()
    {
        if (locker.isWrite())
        {
            for (Tier<?, A> tier : lockedTiers.values())
            {
                tier.getReadWriteLock().readLock().lock();
                tier.getReadWriteLock().writeLock().unlock();
            }
            locker = new ReadLockExtractor();
        }
    }

    // TODO Document.
    public void upgrade()
    {
        if (locker.isWrite())
        {
            throw new IllegalStateException();
        }
        release();
        exclusive();
    }

    // TODO Document.
    public boolean upgrade(Level<T, A> levelOfChild)
    {
        if (!locker.isWrite())
        {
            release();
            // TODO Use Release and Clear.
            levelOfChild.release();
            levelOfChild.lockedTiers.clear();
            exclusive();
            levelOfChild.exclusive();
            return true;
        }
        else if (!levelOfChild.locker.isWrite())
        {
            levelOfChild.release();
            levelOfChild.lockedTiers.clear();
            levelOfChild.exclusive();
            return true;
        }
        return false;
    }
}