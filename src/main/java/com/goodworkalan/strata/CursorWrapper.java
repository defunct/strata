package com.goodworkalan.strata;

public interface CursorWrapper<T, B>
{
    public Cursor<T> wrap(Cursor<B> cursor);
}