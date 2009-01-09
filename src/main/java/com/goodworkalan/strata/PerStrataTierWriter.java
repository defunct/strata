package com.goodworkalan.strata;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PerStrataTierWriter<B, T, F extends Comparable<F>, A, X>
extends AbstractTierCache<B, T, F, A, X>
{
    private final ReadWriteLock readWriteLock;

    public PerStrataTierWriter(Storage<T, F, A, X> storage, Cooper<T, F, B, X> cooper, Extractor<T, F, X> extractor, int max)
    {
        this(storage, cooper, extractor, new ReentrantReadWriteLock(), new Object(), max);
    }

    private PerStrataTierWriter(Storage<T, F, A, X> storage, Cooper<T, F, B, X> cooper, Extractor<T, F, X> extractor,
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

    private PerStrataTierWriter(Storage<T, F, A, X> storage, Cooper<T, F, B, X> cooper, Extractor<T, F, X> extractor,
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
    
    public void end(X txn)
    {
        save(txn, false);
        if (lockCount == 0)
        {
            readWriteLock.readLock().unlock();
        }
    }
    
    public TierWriter<B, A, X> newTierCache()
    {
        return new PerStrataTierWriter<B, T, F, A, X>(getStorage(), cooper, extractor, readWriteLock, mutex, max, isAutoCommit());
    }
}