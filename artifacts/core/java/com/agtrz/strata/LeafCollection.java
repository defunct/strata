/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.AbstractCollection;
import java.util.Iterator;

public class LeafCollection
extends AbstractCollection
{
    private final UrnaryOperator condition;
    
    private final int index;
    
    private final LeafTier leafTier;
    
    public LeafCollection(LeafTier leafTier, int index, UrnaryOperator condition)
    {
        this.leafTier = leafTier;
        this.index = index;
        this.condition = condition;
    }

    public Iterator iterator()
    {
        return new LeafIterator(leafTier, index, condition);
    }

    public int size()
    {
        int size = 0;
        Iterator iterator = iterator();
        while (iterator.hasNext())
        {
            iterator.next();
            size++;
        }
        return size;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */