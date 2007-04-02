/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

/**
 * @author Alan Gutierrez
 */
public interface Tier
{
    public final static short INNER = 1;

    public final static short LEAF = 2;

    public Object getKey();

    public Object getStorageData();

    /**
     * Return true if this tier is full.
     * 
     * @return True if the tier is full.
     */
    public boolean isFull();

    /**
     * Return the number of objects or objects used to pivot in this tier. For
     * an inner tier the size is the number of objects, while the number of
     * child tiers is the size plus one.
     * 
     * @return The size of the tier.
     */
    public int getSize();

    public Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty);

    /**
     * Merge the contents of a tier to the left of this tier into this tier.
     * 
     * @param txn
     *            The transaction of the query.
     * @param left
     *            The tier to the left of this tier.
     */
    public void consume(Object txn, Tier left, Strata.TierSet setOfDirty);
    
    public void revert(Object txn);
    
    public void write(Object txn);

    public void copacetic(Object txn, Strata.Copacetic copacetic);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */