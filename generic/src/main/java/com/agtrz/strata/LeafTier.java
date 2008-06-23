/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;


public interface LeafTier<B, A>
{
    public Tier<B> getTier();
    
    public Cursor<B> find(Comparable<B> comparable);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */