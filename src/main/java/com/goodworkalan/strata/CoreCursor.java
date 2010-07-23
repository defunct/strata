package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A cursor implementation that iterates forward over the leaves of a b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <Record>
 *            The value type of the b+tree objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public final class CoreCursor<Record, Address>
implements Cursor<Record> {
    /** The type-safe container of out of band data. */
    private final Stash stash;

    /** The collection of the core services of the b+tree. */
    private final Structure<Record, Address> structure;
    
    /** The index of the next value returned by the cursor. */
    private int index;

    /** The leaf of the next value returned by the cursor. */
    private Tier<Record, Address> leaf;

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
    public CoreCursor(Stash stash, Structure<Record, Address> structure, Tier<Record, Address> leaf, int index) {
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
    public boolean isForward() {
        return true;
    }

    /**
     * Create a new cursor that starts from the current location of this cursor.
     * 
     * @return A new cursor based on this cursor.
     */
    public Cursor<Record> newCursor() {
        return new CoreCursor<Record, Address>(stash, structure, leaf, index);
    }

  /**
     * Return true if the cursor has more values.
     * 
     * @return True if the cursor has more values.
     */
    public boolean hasNext() {
        return index < leaf.getSize() || !structure.getStorage().isNull(leaf.getNext());
    }

    /**
     * Return the next value in the iteration.
     * 
     * @return the next cursor value.
     */
    public Record next() {
        if (released) {
            throw new IllegalStateException();
        }
        if (index == leaf.getSize()) {
            if (structure.getStorage().isNull(leaf.getNext())) {
                throw new IllegalStateException();
            }
            Tier<Record, Address> next = structure.getPool().get(stash, leaf.getNext());
            next.readWriteLock.readLock().lock();
            next.readWriteLock.readLock().unlock();
            leaf = next;
            index = 0;
        }
        Record object = leaf.getRecord(index++);
        if (!hasNext()) {
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
    public void remove() {
        // FIXME You could attempt to remove, but someone else might remove 
        // it if you let go of the lock. You could try to pick up where
        // you left off, but you'd need to create a set of previous values
        // to exclude. See how concurrent map handles remove in its iterator.
        throw new UnsupportedOperationException();
    }

    /**
     * Release the cursor by releasing the read lock on the current leaf tier.
     */
    public void release() {
        if (!released) {
            leaf.readWriteLock.readLock().unlock();
            released = true;
        }
    }
}