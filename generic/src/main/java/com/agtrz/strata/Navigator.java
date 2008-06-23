/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Navigator<B, A, X>
{
    public LeafTier<B, A> getLeafTier(A address);
    
    public InnerTier<B, A> getInnerTier(A address);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */