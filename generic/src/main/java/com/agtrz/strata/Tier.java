/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.concurrent.locks.ReadWriteLock;

public interface Tier<T>
{
    public ReadWriteLock getReadWriteLock();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */