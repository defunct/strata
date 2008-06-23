/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;


public interface InnerTier<B, A>
{
    public Tier<Branch<B, A>> getTier();
    
    public ChildType getChildType();
    
    public Branch<B, A> find(Comparable<B> comparable);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */