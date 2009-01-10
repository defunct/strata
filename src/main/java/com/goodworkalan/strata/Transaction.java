package com.goodworkalan.strata;

// FIXME Replace X with a Favorites hash.
public interface Transaction<T, F extends Comparable<F>, X>
extends Query<T, F>
{
    public Strata<T, F, X> getStrata();
    
    public X getState();
}