package com.goodworkalan.strata;

// TODO Document.
public interface CursorWrapper<T, B>
{
    // TODO Document.
    public Cursor<T> wrap(Cursor<B> cursor);
}