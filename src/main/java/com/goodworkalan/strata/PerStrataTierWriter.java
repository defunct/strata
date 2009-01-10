package com.goodworkalan.strata;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.goodworkalan.stash.Stash;

public class PerStrataTierWriter<B, T, F extends Comparable<F>, A>
extends AbstractTierCache<B, T, F, A>
{
    private final ReadWriteLock readWriteLock;

    public PerStrataTierWriter(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor, int max)
    {
        this(storage, cooper, extractor, new ReentrantReadWriteLock(), new Object(), max);
    }

    private PerStrataTierWriter(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor,
                                ReadWriteLock readWriteLock,
                                Object mutex,
                                int max)
    {
        this(storage, cooper, extractor,
             readWriteLock,
             mutex,
             max,
             true);
    }

    private PerStrataTierWriter(Storage<T, F, A> storage, Cooper<T, F, B> cooper, Extractor<T, F> extractor,
                                ReadWriteLock readWriteLock,
                                Object mutex,
                                int max,
                                boolean autoCommit)
    {
        super(storage, cooper, extractor, readWriteLock.writeLock(), mutex, max, autoCommit);
        this.readWriteLock = readWriteLock;
    }
    
    public void begin()
    {
        if (lockCount == 0)
        {
            readWriteLock.readLock().lock();
        }
    }
    
    public void end(Stash stash)
    {
        save(stash, false);
        if (lockCount == 0)
        {
            readWriteLock.readLock().unlock();
        }
    }
    
    public TierWriter<B, A> newTierCache()
    {
        return new PerStrataTierWriter<B, T, F, A>(getStorage(), cooper, extractor, readWriteLock, mutex, max, isAutoCommit());
    }
}