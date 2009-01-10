package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;


public final class CoreCursor<B, A>
implements Cursor<B>
{
    private final Stash stash;

    private final Structure<B, A> structure;
    
    private int index;

    private LeafTier<B, A> leaf;

    private boolean released;

    public CoreCursor(Stash stash, Structure<B, A> structure, LeafTier<B, A> leaf, int index)
    {
        this.stash = stash;
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
        return new CoreCursor<B, A>(stash, structure, leaf, index);
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
            LeafTier<B, A> next = structure.getPool().getLeafTier(stash, leaf.getNext());
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