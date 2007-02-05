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

    public boolean isLeaf();

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

    // public void clear();

    public void copacetic(Object txn, Strata.Copacetic copacetic);

    public int getSize();

    public void consume(Object txn, Tier left, Object key);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */