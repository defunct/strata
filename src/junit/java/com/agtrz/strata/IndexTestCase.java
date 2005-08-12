/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Comparator;

import junit.framework.TestCase;

import com.agtrz.sheaf.Sheaf;
import com.agtrz.sheaf.SheafBuilder;
import com.agtrz.sheaf.SheafFactory;
import com.agtrz.sheaf.SheafLocker;
import com.agtrz.sheaf.core.AppendJournal;
import com.agtrz.strata.hash.HashIndexFactory;

/**
 * @author Alan Gutierrez
 */
public class IndexTestCase
extends TestCase
{
    private final static URI INDEX_URI = URI.create("http://agtrz.com/sheaf/2005/08/12/index");
    private final static URI JOURNAL_URI = URI.create("http://agtrz.com/sheaf/2005/08/10/journal");

    private final static String[] ALPHABET = new String[]
    {                                        
        "alpha",
        "beta",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whisky",
        "x-ray",
        "zebra"
    };

    public final static class StringComparator
    implements Comparator
    {
        public int compare(Object o1, Object o2)
        {
            return 0;
        }
        
        public boolean equals(Object obj)
        {
            return false;
        }
    }

    public void _testCreateIndex()
    throws IOException
    {
        File file = File.createTempFile("strata", ".st");
        file.deleteOnExit();
        
        SheafBuilder newSheaf = SheafFactory.INSTANCE.newSheafBuilder();
        
        newSheaf.addJournal(JOURNAL_URI, AppendJournal.class);
        
        IndexFactory factory = HashIndexFactory.INSTANCE;
        
        
        factory.newIndexCreator()
               .create(INDEX_URI, newSheaf, StringComparator.class);
        
        Sheaf sheaf = newSheaf.newSheafCreator(256).create(file);
        SheafLocker locker = sheaf.newSheafLocker(JOURNAL_URI);
        
        Index index = factory.newIndexOpener().open(INDEX_URI, locker);
        
        assertEquals(0, index.getCount());
        assertEquals(StringComparator.class, index.getComparatorClass());

        sheaf.close();
        
        sheaf = SheafFactory.INSTANCE.open(file);
        locker = sheaf.newSheafLocker(JOURNAL_URI);
      
        index = factory.newIndexOpener().open(INDEX_URI, locker);
        
        assertEquals(0, index.getCount());
        assertEquals(StringComparator.class, index.getComparatorClass());
        
        sheaf.close();
    }
    
    private final static class Split
    {
        public final char character;
        public final int index;
        public final Object[] children;
        
        public Split(char character, int index, Object[] children)
        {
            this.character = character;
            this.index = index;
            this.children = children;
        }
    }
    
    private final static void insert(Split[] splits, String string)
    {
        int i;
        int index = 0;
        for (i = 0; i < splits.length && splits[i] != null; i++)
        {
            
        }
        if (i == 0)
        {
            splits[0] = new Split(string.charAt(index), index, new String[3]);
            splits[0].children[0] = string;
        }
    }
    
    private final static boolean hasSplit(Split[] splits, int i)
    {
        return i < splits.length && splits[i] != null;
    }
    
    public final static boolean hasString(String[] strings, int i)
    {
        return i < strings.length && strings[i] != null;
    }
    
    private final static boolean scan(String[] strings, String string)
    {
        for (int i = 0; hasString(strings, i); i++)
        {
            if (string.equals(strings[i]))
            {
                return true;
            }
        }
        return false;
    }

    private final static boolean contains(Split[] splits, String string)
    {
        int index = 0;
        for (int i = 0; hasSplit(splits, i); i++)
        {
            Split split = splits[i];
            if (string.charAt(index) == split.character)
            {
                if (split.children instanceof String[])
                {
                    return scan((String[]) split.children, string);
                }
            }
        }
        return false;
    }
    
    private final static Split[] newSplits()
    {
        return new Split[3];
    }
    
    public void testInsertFirstString()
    {
       Split[] splits = newSplits();
       insert(splits, "bad");
       assertTrue(contains(splits, "bad"));
    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */