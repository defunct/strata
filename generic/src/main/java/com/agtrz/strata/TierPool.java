/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface TierPool<A, T, X>
{
    public Tier<T> getTier(X txn, A address);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */