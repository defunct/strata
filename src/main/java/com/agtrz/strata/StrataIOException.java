/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.Danger;

/**
 * @author Alan Gutierez
 */
public class StrataIOException
extends StrataException
{
    public StrataIOException(Danger danger, Throwable cause)
    {
        super(danger, cause);
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
