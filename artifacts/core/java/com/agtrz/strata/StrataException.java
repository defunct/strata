/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import com.agtrz.swag.danger.Danger;

/**
 * @author Alan Gutierez
 */
public class StrataException
extends Danger
{
    private final static long serialVersionUID = 20051009L;

    public StrataException()
    {
        super();
    }

    public StrataException(Throwable cause)
    {
        super(cause);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */