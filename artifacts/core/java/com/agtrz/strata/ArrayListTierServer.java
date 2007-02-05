/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.swag.util.IdentityObject;

public class ArrayListTierServer
implements Storage
{
    public TierLoader getInnerPageLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object storage, Object key)
            {
                return (InnerTier) ((IdentityObject) key).getObject();
            }
        };
    }

    public TierLoader getLeafPageLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object storage, Object key)
            {
                return (LeafTier) ((IdentityObject) key).getObject();
            }
        };

    }

    public InnerTier newInnerPage(Strata.Structure structure, Object storage, short typeOfChildren)
    {
        return new ArrayListInnerTier(structure, typeOfChildren);
    }

    public LeafTier newLeafPage(Strata.Structure structure, Object storage)
    {
        return new ArrayListLeafTier(structure);
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