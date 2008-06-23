/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface CursorWrapper<T, B>
{
    public Cursor<T> wrap(Cursor<B> cursor);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */