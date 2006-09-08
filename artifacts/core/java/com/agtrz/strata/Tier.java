/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.util.Comparator;

/**
 * @author Alan Gutierrez
 */
public interface Tier
{
    public Branch insert(Stratifier stratifier, Comparator comparator, Object object);
    
    public boolean isLeaf();
}


/* vim: set et sw=4 ts=4 ai tw=70: */