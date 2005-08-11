/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import com.agtrz.sheaf.SheafBuilder;
import com.agtrz.sheaf.SheafFactory;
import com.agtrz.sheaf.core.AppendJournal;

/**
 * @author Alan Gutierrez
 */
public class IndexTestCase
{
    private final static URI JOURNAL_URI = URI.create("http://agtrz.com/sheaf/2005/08/10/journal");
    
    public void testCreateIndex()
    throws IOException
    {
        File file = File.createTempFile("strata", ".st");
        file.deleteOnExit();
        
        SheafBuilder newSheaf = SheafFactory.INSTANCE.newSheafBuilder();
        
        newSheaf.addJournal(JOURNAL_URI, AppendJournal.class);
//        Index index = IndexFactory.newIndex(file);
    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */