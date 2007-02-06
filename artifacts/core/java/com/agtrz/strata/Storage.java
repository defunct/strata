/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Storage
{
    public TierLoader getInnerTierLoader();

    public TierLoader getLeafTierLoader();
    
    public InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren);
    
    public LeafTier newLeafTier(Strata.Structure structure, Object txn);
    
    public Object getNullKey();
    
    public boolean isKeyNull(Object object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */