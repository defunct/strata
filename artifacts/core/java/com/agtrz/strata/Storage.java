/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.Comparator;

import com.agtrz.swag.util.Equator;

public class Storage
{
    private final Pager pager;

    private final Comparator comparator;

    private final Object store;

    private final Equator equator;

    private final int size;

    public Storage(Pager pager, Object store, Comparator comparator, Equator equator, int size)
    {
        this.pager = pager;
        this.store = store;
        this.comparator = comparator;
        this.equator = equator;
        this.size = size;
    }

    public Pager getPager()
    {
        return pager;
    }

    public Comparator getComparator()
    {
        return comparator;
    }

    public Object getStore()
    {
        return store;
    }

    public Equator getEquator()
    {
        return equator;
    }

    public int getSize()
    {
        return size;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */