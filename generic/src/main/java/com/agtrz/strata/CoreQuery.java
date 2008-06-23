/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class CoreQuery<T, F extends Comparable<? super F>, A, X, B>
{
    private Navigator<B, A, X> navigator;
    
    public abstract InnerTier<B, A> getRoot();
    
    private CursorWrapper<T, B> cursorWrapper;
        
    // Here is where I get the power of not using comparator.
    public Cursor<T> find(F fields)
    {
        Lock previous = new ReentrantLock();
        previous.lock();
        InnerTier<B, A> inner = getRoot();
        Comparable<B> comparator = new BucketComparable<T, F, B, X>();
        for (;;)
        {
            inner.getTier().getReadWriteLock().readLock().lock();
            previous.unlock();
            previous = inner.getTier().getReadWriteLock().readLock();
            Branch<B, A> branch = inner.find(comparator);
            if (inner.getChildType() == ChildType.LEAF)
            {
                LeafTier<B, A> leaf = navigator.getLeafTier(branch.getRightKey());
                leaf.getTier().getReadWriteLock().readLock().lock();
                previous.unlock();
                return cursorWrapper.wrap(leaf.find(comparator));
            }
            inner = navigator.getInnerTier(branch.getRightKey());
        }
    }

}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */