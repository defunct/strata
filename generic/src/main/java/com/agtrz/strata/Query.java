/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Query<T, F extends Comparable<? super F>>
{
    public void add(T object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */