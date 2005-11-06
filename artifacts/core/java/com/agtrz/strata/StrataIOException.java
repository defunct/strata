/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.danger.Danger;

/**
 * @author Alan Gutierez
 */
public class StrataIOException
extends StrataException
{
    private final static long serialVersionUID = 20051009L;

    public StrataIOException(Danger danger, Throwable cause)
    {
        super(danger, cause);
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
