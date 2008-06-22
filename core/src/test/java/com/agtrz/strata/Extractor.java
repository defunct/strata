/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Extractor<O, E extends Comparable<? super E>>
{
    E extract(O o);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */