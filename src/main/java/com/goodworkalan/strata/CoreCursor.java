package com.goodworkalan.strata;


public final class CoreCursor<B, A, X>
implements Cursor<B>
{
    private final X txn;

    private final Structure<B, A, X> structure;
    
    private int index;

    private LeafTier<B, A> leaf;

    private boolean released;

    public CoreCursor(X txn, Structure<B, A, X> structure, LeafTier<B, A> leaf, int index)
    {
        this.txn = txn;
        this.structure = structure;
        this.leaf = leaf;
        this.index = index;
    }

    public boolean isForward()
    {
        return true;
    }

    public Cursor<B> newCursor()
    {
        return new CoreCursor<B, A, X>(txn, structure, leaf, index);
    }

    public boolean hasNext()
    {
        return index < leaf.size() || !structure.getAllocator().isNull(leaf.getNext());
    }

    public B next()
    {
        if (released)
        {
            throw new IllegalStateException();
        }
        if (index == leaf.size())
        {
            if (structure.getAllocator().isNull(leaf.getNext()))
            {
                throw new IllegalStateException();
            }
            LeafTier<B, A> next = structure.getPool().getLeafTier(txn, leaf.getNext());
            next.getReadWriteLock().readLock().lock();
            leaf.getReadWriteLock().readLock().unlock();
            leaf = next;
            index = 0;
        }
        B object = leaf.get(index++);
        if (!hasNext())
        {
            release();
        }
        return object;
    }
    
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void release()
    {
        if (!released)
        {
            leaf.getReadWriteLock().readLock().unlock();
            released = true;
        }
    }
}