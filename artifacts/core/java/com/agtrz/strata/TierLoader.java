/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface TierLoader
{
    public Tier load(Strata.Structure structure, Object storage, Object key);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */