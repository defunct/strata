package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A cursor implementation that iterates forward over the leaves of a b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class CoreCursor<T, A>
implements Cursor<T>
{
    /**  The type-safe container of out of band data. */
    private final Stash stash;

    /** The collection of the core services of the b+tree. */
    private final Structure<T, A> structure;
    
    /** The index of the next value returned by the cursor. */
    private int index;

    /** The leaf of the next value returned by the cursor. */
    private LeafTier<T, A> leaf;

    /**
     * True if the cursor has been released and the read lock on the current
     * leaf tier has been released.
     */
    private boolean released;

    /**
     * Create a new cursor.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param structure
     *            A collection of the core services of the b+tree.
     * @param leaf
     *            The leaf containing the first value returned by the cursor.
     * @param index
     *            The index of the first value returned by the cursor.
     */
    public CoreCursor(Stash stash, Structure<T, A> structure, LeafTier<T, A> leaf, int index)
    {
        this.stash = stash;
        this.structure = structure;
        this.leaf = leaf;
        this.index = index;
    }

    /**
     * Return true indicating that this is a cursor that iterators forward over
     * the leaves of a b+tree.
     * 
     * @return True to indicate that this is a forward cursor.
     */
    public boolean isForward()
    {
        return true;
    }

    /**
     * Create a new cursor that starts from the current location of this cursor.
     * 
     * @return A new cursor based on this cursor.
     */
    public Cursor<T> newCursor()
    {
        return new CoreCursor<T, A>(stash, structure, leaf, index);
    }

    /**
     * Return true if the cursor has more values.
     * 
     * @return True if the cursor has more values.
     */
    public boolean hasNext()
    {
        return index < leaf.size() || !structure.getAllocator().isNull(leaf.getNext());
    }

    /**
     * Return the next value in the iteration.
     * 
     * @return the next cursor value.
     */
    public T next()
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
            LeafTier<T, A> next = structure.getPool().getLeafTier(stash, leaf.getNext());
            next.getReadWriteLock().readLock().lock();
            leaf.getReadWriteLock().readLock().unlock();
            leaf = next;
            index = 0;
        }
        T object = leaf.get(index++);
        if (!hasNext())
        {
            release();
        }
        return object;
    }

    /**
     * The remove operation is unsupported.
     * 
     * @exception UnsupportedOperationException
     *                Thrown to indicate that the remove operation is not
     *                supported.
     */
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Release the cursor by releasing the read lock on the current leaf tier.
     */
    public void release()
    {
        if (!released)
        {
            leaf.getReadWriteLock().readLock().unlock();
            released = true;
        }
    }
}