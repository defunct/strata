/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;


/**
 * @author Alan Gutierrez
 */
public interface IndexFactory
{
    public IndexCreator newIndexCreator();
    
    public IndexOpener newIndexOpener();
}


/* vim: set et sw=4 ts=4 ai tw=70: */