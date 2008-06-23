/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public final class Bucket<T, F extends Comparable<? super F>>
{
    public final F fields;

    public final T object;

    public Bucket(F fields, T object)
    {
        this.fields = fields;
        this.object = object;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */