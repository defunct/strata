package com.goodworkalan.strata;


public interface Transaction<T, F extends Comparable<F>, X>
extends Query<T, F>
{
    public Strata<T, F, X> getStrata();
    
    public X getState();
}