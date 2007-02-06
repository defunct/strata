/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.strata.Strata.Structure;
import com.agtrz.swag.util.IdentityObject;

public class ArrayListStorage
implements Storage
{
    private final TierLoader innerTierLoader = new TierLoader()
    {
        public Tier load(Strata.Structure structure, Object storage, Object key)
        {
            if (key != null)
            {
                return (InnerTier) ((IdentityObject) key).getObject();
            }
            return null;
        }

    };

    private final TierLoader leafTierLoader = new TierLoader()
    {
        public Tier load(Strata.Structure structure, Object storage, Object key)
        {
            if (key != null)
            {
                return (LeafTier) ((IdentityObject) key).getObject();
            }
            return null;
        }
    };

    public TierLoader getInnerTierLoader()
    {
        return innerTierLoader;
    }

    public TierLoader getLeafTierLoader()
    {
        return leafTierLoader;
    }

    public InnerTier newInnerTier(Strata.Structure structure, Object storage, short typeOfChildren)
    {
        return new ArrayListInnerTier(structure, typeOfChildren);
    }

    public LeafTier newLeafTier(Strata.Structure structure, Object storage)
    {
        return new ArrayListLeafTier(structure);
    }

    public void write(Structure structure, Object txn, InnerTier inner)
    {
    }

    public void write(Structure structure, Object txn, LeafTier leaf)
    {
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