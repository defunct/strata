/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.Danger;
import com.agtrz.swag.UniversalException;

/**
 * @author Alan Gutierez
 */
public class StrataException
extends UniversalException
{
    public StrataException(Danger danger)
    {
        super(danger);
    }

    public StrataException(Danger danger, Throwable cause)
    {
        super(danger, cause);
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */