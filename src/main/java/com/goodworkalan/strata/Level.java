package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

// TODO Document.
final class Level<B, A>
{
    // TODO Document.
    public LockExtractor getSync;

    // TODO Document.
    public final Map<Object, Tier<?, A>> mapOfLockedTiers = new HashMap<Object, Tier<?, A>>();

    // TODO Document.
    public final LinkedList<Operation<B, A>> listOfOperations = new LinkedList<Operation<B, A>>();

    // TODO Document.
    public Level(boolean exclusive)
    {
        this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
    }

    // TODO Document.
    public void lockAndAdd(Tier<?, A> tier)
    {
        lock_(tier);
        add_(tier);
    }
    
    // TODO Document.
    public void unlockAndRemove(Tier<?, A> tier)
    {
        assert mapOfLockedTiers.containsKey(tier.getAddress());
        
        mapOfLockedTiers.remove(tier.getAddress());
        unlock_(tier);
    }

    // TODO Document.
    public void add_(Tier<?, A> tier)
    {
        mapOfLockedTiers.put(tier.getAddress(), tier);
    }

    // TODO Document.
    public void lock_(Tier<?, A> tier)
    {
        getSync.getSync(tier.getReadWriteLock()).lock();
    }

    // TODO Document.
    public void unlock_(Tier<?, A> tier)
    {
        getSync.getSync(tier.getReadWriteLock()).unlock();
    }

    // TODO Document.
    public void release()
    {
        Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
        while (lockedTiers.hasNext())
        {
            Tier<?, A> tier = lockedTiers.next();
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }
    }

    // TODO Document.
    public void releaseAndClear()
    {
        Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
        while (lockedTiers.hasNext())
        {
            Tier<?, A> tier = lockedTiers.next();
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }
        mapOfLockedTiers.clear();
    }

    // TODO Document.
    private void exclusive()
    {
        Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
        while (lockedTiers.hasNext())
        {
            Tier<?, A> tier = lockedTiers.next();
            tier.getReadWriteLock().writeLock().lock();
        }
        getSync = new WriteLockExtractor();
    }

    // TODO Document.
    public void downgrade()
    {
        if (getSync.isExeclusive())
        {
            Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
            while (lockedTiers.hasNext())
            {
                Tier<?, A> tier = lockedTiers.next();
                tier.getReadWriteLock().readLock().lock();
                tier.getReadWriteLock().writeLock().unlock();
            }
            getSync = new ReadLockExtractor();
        }
    }

    // TODO Document.
    public void upgrade()
    {
        if (getSync.isExeclusive())
        {
            throw new IllegalStateException();
        }
        release();
        exclusive();
    }

    // TODO Document.
    public boolean upgrade(Level<B, A> levelOfChild)
    {
        if (!getSync.isExeclusive())
        {
            release();
            // TODO Use Release and Clear.
            levelOfChild.release();
            levelOfChild.mapOfLockedTiers.clear();
            exclusive();
            levelOfChild.exclusive();
            return true;
        }
        else if (!levelOfChild.getSync.isExeclusive())
        {
            levelOfChild.release();
            levelOfChild.mapOfLockedTiers.clear();
            levelOfChild.exclusive();
            return true;
        }
        return false;
    }
}