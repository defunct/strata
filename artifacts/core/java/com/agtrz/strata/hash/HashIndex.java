/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata.hash;

import com.agtrz.strata.Index;

/**
 * @author Alan Gutierez
 */
final class HashIndex
implements Index
{
    private final Class comparatorClass;
    
    public HashIndex(Class comparatorClass)
    {
        this.comparatorClass = comparatorClass;
    }

    public Class getComparatorClass()
    {
        return comparatorClass;
    }
    
    public int getCount()
    {
        return 0;
    }
    
    public void add(long address, Object object)
    {
    }
    
    public long get(Comparable comparable)
    {
        return 0;
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
