/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

// FIXME REname.
public interface PageLoader
{
    public Tier load(Storage storage, Object key);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */