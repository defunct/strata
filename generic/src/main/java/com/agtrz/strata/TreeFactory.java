/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface TreeFactory<T, F extends Comparable<? super F>, X>
{
    Tree<T, F, X> newTree();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */