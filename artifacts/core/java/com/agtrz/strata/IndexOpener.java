/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import java.net.URI;

import com.agtrz.sheaf.SheafLocker;

/**
 * @author Alan Gutierez
 */
public interface IndexOpener
{
    public Index open(URI uriOfIndex, SheafLocker locker);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
