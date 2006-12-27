/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Iterator;

public class LeafIterator
implements Iterator
{
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
    public LeafIterator(LeafTier leafTier, int index, UrnaryOperator condition)
    {
        if (!condition.operate(leafTier.listOfObjects.get(index)))
        {
            throw new IllegalArgumentException();
        }
        this.current = leafTier.listOfObjects.get(index);
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
        if (index == leafTier.listOfObjects.size())
        {
            leafTier = leafTier.nextLeafTier;
            index = 0;
        }
        if (leafTier == null)
        {
            current = null;
        }
        else if (index < leafTier.listOfObjects.size())
        {
            Object object = leafTier.listOfObjects.get(index);
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