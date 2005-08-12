/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata.hash;

import com.agtrz.strata.IndexCreator;
import com.agtrz.strata.IndexFactory;
import com.agtrz.strata.IndexOpener;

/**
 * @author Alan Gutierez
 */
public class HashIndexFactory
implements IndexFactory
{
    public final static HashIndexFactory INSTANCE = new HashIndexFactory();
    
    private HashIndexFactory()
    {
    }
    
    public IndexCreator newIndexCreator()
    {
        return new HashIndexCreator();
    }
    
    public IndexOpener newIndexOpener()
    {
        return new HashIndexOpener();
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
