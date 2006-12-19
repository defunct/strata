/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import java.net.URI;

import com.agtrz.sheaf.SheafBuilder;

/**
 * @author Alan Gutierez
 */
public interface IndexCreator
{
    public void create(URI uriOfIndex, SheafBuilder newSheaf, Class comparitorClass);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
