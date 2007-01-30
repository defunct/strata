/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

/**
 * @author Alan Gutierrez
 */
public interface Tier
{
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
    public Split split(Object object);

//    public void clear();

    public void copacetic(Strata.Copacetic copacetic);
    
    public int size();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */