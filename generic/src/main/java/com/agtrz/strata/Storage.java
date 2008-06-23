/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Storage<A, T, X>
{
    public Store<A, Short, Branch<T, A>, X> getBranchStore();
    
    public Store<A, A, T, X> getLeafStore();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */