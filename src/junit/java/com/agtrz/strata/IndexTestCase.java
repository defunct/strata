/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;

/**
 * @author Alan Gutierrez
 */
public class IndexTestCase
{
    public void testIndex()
    throws IOException
    {
        File file = File.createTempFile("strata", ".st");
        file.deleteOnExit();
//        Index index = IndexFactory.newIndex(file);
    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */