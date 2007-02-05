/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.AbstractCollection;
import java.util.Iterator;

public class LeafCollection
extends AbstractCollection
{
    private final Strata.Structure structure;

    private final Object txn;

    private final Strata.Criteria criteria;

    private final int index;

    private final LeafTier leafTier;

    public LeafCollection(Strata.Structure structure, Object txn, LeafTier leafTier, int index, Strata.Criteria criteria)
    {
        this.structure = structure;
        this.txn = txn;
        this.leafTier = leafTier;
        this.index = index;
        this.criteria = criteria;
    }

    public Iterator iterator()
    {
        return new LeafIterator(structure, txn, leafTier, index, criteria);
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