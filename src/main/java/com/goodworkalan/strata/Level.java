package com.goodworkalan.strata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;


final class Level<B, A>
{
    public LockExtractor getSync;

    public final Map<Object, Tier<?, A>> mapOfLockedTiers = new HashMap<Object, Tier<?, A>>();

    public final LinkedList<Operation<B, A>> listOfOperations = new LinkedList<Operation<B, A>>();

    public Level(boolean exclusive)
    {
        this.getSync = exclusive ? (LockExtractor) new WriteLockExtractor() : (LockExtractor) new ReadLockExtractor();
    }

    public void lockAndAdd(Tier<?, A> tier)
    {
        lock_(tier);
        add_(tier);
    }
    
    public void unlockAndRemove(Tier<?, A> tier)
    {
        assert mapOfLockedTiers.containsKey(tier.getAddress());
        
        mapOfLockedTiers.remove(tier.getAddress());
        unlock_(tier);
    }

    public void add_(Tier<?, A> tier)
    {
        mapOfLockedTiers.put(tier.getAddress(), tier);
    }

    public void lock_(Tier<?, A> tier)
    {
        getSync.getSync(tier.getReadWriteLock()).lock();
    }

    public void unlock_(Tier<?, A> tier)
    {
        getSync.getSync(tier.getReadWriteLock()).unlock();
    }

    public void release()
    {
        Iterator<Tier<?, A>> lockedTiers = mapOfLockedTiers.values().iterator();
        while (lockedTiers.hasNext())
        {
            Tier<?, A> tier = lockedTiers.next();
            getSync.getSync(tier.getReadWriteLock()).unlock();
        }
    }

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

    public void upgrade()
    {
        if (getSync.isExeclusive())
        {
            throw new IllegalStateException();
        }
        release();
        exclusive();
    }

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