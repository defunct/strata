/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import com.agtrz.swag.util.IdentityObject;

public class ArrayListPager
implements Pager
{
    public PageLoader getInnerPageLoader()
    {
        return new PageLoader()
        {
            public Tier load(Storage storage, Object key)
            {
                return (InnerTier) ((IdentityObject) key).getObject();
            }
        };
    }

    public PageLoader getLeafPageLoader()
    {
        return new PageLoader()
        {
            public Tier load(Storage storage, Object key)
            {
                return (LeafTier) ((IdentityObject) key).getObject();
            }
        };

    }

    public InnerTier newInnerPage(Storage storage, short typeOfChildren)
    {
        return new ArrayListInnerPage(storage, storage.getSize(), typeOfChildren);
    }

    public LeafTier newLeafPage(Storage storage)
    {
        return new ArrayListLeafPage(storage);
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