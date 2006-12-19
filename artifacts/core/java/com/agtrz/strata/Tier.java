/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.util.Comparator;

/**
 * @author Alan Gutierrez
 */
public interface Tier
{
    public boolean isLeaf();

    public boolean isFull();

    /**
     * Split a tier into two tiers.
     * <p>
     * The
     * 
     * @param comparator
     * @return
     */
    public Split split(Comparator comparator);

    public void clear();

    public void copacetic(Comparator comparator);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */