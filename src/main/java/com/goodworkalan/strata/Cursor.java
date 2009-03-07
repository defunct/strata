package com.goodworkalan.strata;

import java.util.Iterator;

// TODO Document.
public interface Cursor<T>
extends Iterator<T>
{
    // TODO Document.
    public void release();
}