package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class CoreCursor<B, A>
implements Cursor<B>
{
    // TODO Document.
    private final Stash stash;

    // TODO Document.
    private final Structure<B, A> structure;
    
    // TODO Document.
    private int index;

    // TODO Document.
    private LeafTier<B, A> leaf;

    // TODO Document.
    private boolean released;

    // TODO Document.
    public CoreCursor(Stash stash, Structure<B, A> structure, LeafTier<B, A> leaf, int index)
    {
        this.stash = stash;
        this.structure = structure;
        this.leaf = leaf;
        this.index = index;
    }

    // TODO Document.
    public boolean isForward()
    {
        return true;
    }

    // TODO Document.
    public Cursor<B> newCursor()
    {
        return new CoreCursor<B, A>(stash, structure, leaf, index);
    }

    // TODO Document.
    public boolean hasNext()
    {
        return index < leaf.size() || !structure.getAllocator().isNull(leaf.getNext());
    }

    // TODO Document.
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
    
    // TODO Document.
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // TODO Document.
   public void release()
    {
        if (!released)
        {
            leaf.getReadWriteLock().readLock().unlock();
            released = true;
        }
    }
}