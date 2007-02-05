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

    /**
     * Return true if this tier is full.
     * 
     * @return True if the tier is full.
     */
    public boolean isFull();

    /**
     * Split a tier into two tiers.
     * 
     * @param comparator
     *            The comparator to use to compare objects.
     * @return A <tt>Split</tt> object containing the partition object and
     *         the new right and left containers.
     */
    public Split split(Object txn, Strata.Criteria criteria);

    public void copacetic(Object txn, Strata.Copacetic copacetic);

    /**
     * Return the number of objects or objects used to pivot in this tier. For
     * an inner tier the size is the number of objects, while the number of
     * child tiers is the size plus one.
     * 
     * @return The size of the tier.
     */
    public int getSize();

    /**
     * Merge the contents of a tier to the left of this tier into this tier.
     * 
     * @param txn
     *            The transaction of the query.
     * @param left
     *            The tier to the left of this tier.
     */
    public void consume(Object txn, Tier left);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */