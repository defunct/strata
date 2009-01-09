package com.goodworkalan.strata;

import java.util.Iterator;

public interface Cursor<T>
extends Iterator<T>
{
    public void release();
}