/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Iterator;

public class LeafIterator
implements Iterator
{
    private final Strata.Structure structure;

    private final Object txn;

    private final Strata.Criteria criteria;

    private int index;

    private LeafTier leaf;

    private Object current;

    /**
     * Create a new leaf iterator for the specified condition offset into a
     * leaf tier.
     * 
     * @param leaf
     *            The leaf tier to iterate.
     * @param index
     *            The start index.
     * @param condition
     *            The condition that determines if the object is included by
     *            the iterator.
     */
    public LeafIterator(Strata.Structure structure, Object txn, LeafTier leaf, int index, Strata.Criteria criteria)
    {
        if (!criteria.exactMatch(leaf.get(index)))
        {
            throw new IllegalArgumentException();
        }
        this.structure = structure;
        this.txn = txn;
        this.current = leaf.get(index);
        this.leaf = leaf;
        this.index = index + 1;
        this.criteria = criteria;
    }

    public boolean hasNext()
    {
        return current != null;
    }

    public Object next()
    {
        if (current == null)
        {
            throw new IllegalStateException();
        }
        Object next = current;
        if (index == leaf.getSize())
        {
            Storage storage = structure.getStorage();
            if (storage.isKeyNull(leaf.getNextLeafTier()))
            {
                leaf = null;
            }
            else
            {
                leaf = (LeafTier) storage.getLeafPageLoader().load(structure, txn, leaf.getNextLeafTier());
            }
            index = 0;
        }
        if (leaf == null)
        {
            current = null;
        }
        else if (index < leaf.getSize())
        {
            Object object = leaf.get(index);
            if (criteria.exactMatch(object))
            {
                current = object;
                index++;
            }
            else
            {
                current = null;
            }
        }
        else
        {
            current = null;
        }
        return next;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */