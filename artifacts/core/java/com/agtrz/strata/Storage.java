/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.Serializable;

public interface Storage extends Serializable
{
    public TierLoader getInnerTierLoader();

    public TierLoader getLeafTierLoader();

    public InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren);

    public LeafTier newLeafTier(Strata.Structure structure, Object txn);

    public void write(Strata.Structure structure, Object txn, InnerTier inner);

    public void write(Strata.Structure structure, Object txn, LeafTier leaf);
    
    public void free(Strata.Structure structure, Object txn, InnerTier inner);
    
    public void free(Strata.Structure structure, Object txn, LeafTier leaf);

    public Object getNullKey();

    public boolean isKeyNull(Object object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */