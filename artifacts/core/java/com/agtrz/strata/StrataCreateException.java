/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.Danger;

/**
 * @author Alan Gutierez
 */
public class StrataCreateException
extends StrataException
{
    private final static long serialVersionUID = 20051009L;

    public StrataCreateException(Danger danger)
    {
        super(danger);
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
