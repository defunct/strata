/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.io.File;

import com.agtrz.strata.core.CoreIndexFactory;

/**
 * @author Alan Gutierrez
 */
public class IndexFactory
{
    public static Index newIndex(File file)
    {
        return CoreIndexFactory.newIndex(file);
    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */