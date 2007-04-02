/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.strata.Strata.Structure;

public class ArrayListStorage
implements Storage
{
    private static final long serialVersionUID = 20070208L;

    public InnerTier getInnerTier(Structure structure, Object txn, Object key)
    {
        return (InnerTier) key;
    }

    public LeafTier getLeafTier(Structure structure, Object txn, Object key)
    {
        return (LeafTier) key;
    }

    public InnerTier newInnerTier(Strata.Structure structure, Object storage, short typeOfChildren)
    {
        InnerTier inner = new InnerTier(structure, null);
        inner.setChildType(typeOfChildren);
        return inner;
    }

    public LeafTier newLeafTier(Strata.Structure structure, Object storage)
    {
        return new LeafTier(structure, null);
    }

    public void write(Structure structure, Object txn, InnerTier inner)
    {
    }

    public void write(Structure structure, Object txn, LeafTier leaf)
    {
    }

    public void free(Structure structure, Object txn, InnerTier inner)
    {
    }

    public void free(Structure structure, Object txn, LeafTier leaf)
    {
    }

    public void revert(Structure structure, Object txn, InnerTier inner)
    {
    }

    public void revert(Structure structure, Object txn, LeafTier leaf)
    {
    }

    public Object getKey(Tier leaf)
    {
        return leaf;
    }

    public Object getNullKey()
    {
        return null;
    }

    public boolean isKeyNull(Object object)
    {
        return object == null;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */