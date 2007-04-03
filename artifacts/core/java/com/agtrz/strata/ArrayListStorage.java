/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.strata.Strata.Structure;

public class ArrayListStorage
implements Strata.Storage
{
    private static final long serialVersionUID = 20070208L;

    public Strata.InnerTier getInnerTier(Structure structure, Object txn, Object key)
    {
        return (Strata.InnerTier) key;
    }

    public Strata.LeafTier getLeafTier(Structure structure, Object txn, Object key)
    {
        return (Strata.LeafTier) key;
    }

    public Strata.InnerTier newInnerTier(Strata.Structure structure, Object storage, short typeOfChildren)
    {
        return new Strata.InnerTier(structure, null, typeOfChildren);
    }

    public Strata.LeafTier newLeafTier(Strata.Structure structure, Object storage)
    {
        return new Strata.LeafTier(structure, null);
    }

    public void write(Structure structure, Object txn, Strata.InnerTier inner)
    {
    }

    public void write(Structure structure, Object txn, Strata.LeafTier leaf)
    {
    }

    public void free(Structure structure, Object txn, Strata.InnerTier inner)
    {
    }

    public void free(Structure structure, Object txn, Strata.LeafTier leaf)
    {
    }

    public void revert(Structure structure, Object txn, Strata.InnerTier inner)
    {
    }

    public void revert(Structure structure, Object txn, Strata.LeafTier leaf)
    {
    }

    public Object getKey(Strata.Tier leaf)
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