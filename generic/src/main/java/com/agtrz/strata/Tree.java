/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Tree<T, F extends Comparable<? super F>, X>
{
    public Query<T, F> query(X txn);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */