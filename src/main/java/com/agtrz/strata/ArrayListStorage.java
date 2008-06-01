/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.strata.Strata.Storage;

public class ArrayListStorage
implements Strata.Storage
{
    private static final long serialVersionUID = 20070208L;

    public Strata.InnerTier getInnerTier(Strata.Structure structure, Object txn, Object key)
    {
        return (Strata.InnerTier) key;
    }

    public Strata.LeafTier getLeafTier(Strata.Structure structure, Object txn, Object key)
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

    public void write(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
    }

    public void write(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
    }

    public void free(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
    }

    public void free(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
    }

    public void commit(Object txn)
    {
    }

    public Object getKey(Strata.Tier leaf)
    {
        return leaf;
    }

    public boolean isKeyNull(Object object)
    {
        return object == null;
    }

    public Object getNullKey()
    {
        return null;
    }

    public Strata.Storage.Schema getSchema()
    {
        return new Schema();
    }

    public final static class Schema
    implements Strata.Storage.Schema
    {
        public Storage newStorage()
        {
            return new ArrayListStorage();
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
