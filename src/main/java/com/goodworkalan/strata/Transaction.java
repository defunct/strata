package com.goodworkalan.strata;


public interface Transaction<T, X>
extends Query<T>
{
    public Strata<T, X> getStrata();
    
    public X getState();
}