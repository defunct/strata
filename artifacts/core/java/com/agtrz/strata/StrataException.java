/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.danger.Danger;
import com.agtrz.swag.danger.UniversalException;

/**
 * @author Alan Gutierez
 */
public class StrataException
extends UniversalException
{
    private final static long serialVersionUID = 20051009L;

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
