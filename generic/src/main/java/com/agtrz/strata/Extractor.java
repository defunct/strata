/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Extractor<T, F extends Comparable<? super F>, X>
{
    F extract(X txn, T object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */