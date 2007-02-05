/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Iterator;

import com.agtrz.operators.UrnaryOperator;

public class LeafIterator
implements Iterator
{
    private final Storage storage;

    private final UrnaryOperator condition;

    private int index;

    private LeafTier leafTier;

    private Object current;

    /**
     * Create a new leaf iterator for the specified condition offset into a
     * leaf tier.
     * 
     * @param leafTier
     *            The leaf tier to iterate.
     * @param index
     *            The start index.
     * @param condition
     *            The condition that determines if the object is included by
     *            the iterator.
     */
    public LeafIterator(Storage storage, LeafTier leafTier, int index, UrnaryOperator condition)
    {
        if (!condition.operate(leafTier.get(index)))
        {
            throw new IllegalArgumentException();
        }
        this.storage = storage;
        this.current = leafTier.get(index);
        this.leafTier = leafTier;
        this.index = index + 1;
        this.condition = condition;
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
        if (index == leafTier.getSize())
        {
            leafTier = (LeafTier) storage.getPager().getLeafPageLoader().load(storage, leafTier.getNextLeafTier());
            index = 0;
        }
        if (leafTier == null)
        {
            current = null;
        }
        else if (index < leafTier.getSize())
        {
            Object object = leafTier.get(index);
            if (condition.operate(object))
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