/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

/**
 * @author Alan Gutierrez
 */
public interface Index
{
    public int getCount();

    public Class getComparatorClass();

    public void add(long address, Object object);

    public long get(Comparable comparable);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */