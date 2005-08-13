/*
 * Copyright Alan Gutierrez - 2005 - All Rights Reserved
 */
package com.agtrz.strata;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
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
    
    private final static int SPLITS_LENGTH = 3;
    
    private final static int STRINGS_LENGTH = 3;

    private final static void insert(Split[] splits, String string)
    {
        if (splits[1] == null)
        {
            splits[1] = splits[0];
            splits[0] = new Split(string.charAt(0), 0, new String[STRINGS_LENGTH]);
            splits[0].children[0] = string;
        }
        else
        {
            insert(splits, string, 0);
        }
    }
    
    private final static void insert(Split[] splits, String string, int index)
    {
        int i;
        for (i = 0; hasSplit(splits, i); i++)
        {
            Split split = splits[i];
            if (string.charAt(index) <= split.character)
            {
                break;
            }
        }
        
        if (i == SPLITS_LENGTH)
        {
            throw new IllegalStateException();
        }
        
        Split split = splits[i];
        if (split.children instanceof String[])
        {
            String[] strings = (String[]) splits[i].children;
            if (!hasSlot(strings, string))
            {
                char ch = string.charAt(index);
                int j = 0;
                for (;;)
                {
                    if (strings.length == j)
                    {
                        break;
                    }

                    if (strings[j].length() == index)
                    {
                        throw new UnsupportedOperationException();
                    }

                    if (ch <= strings[j].charAt(index))
                    {
                        break;
                    }
                    
                    j++;
                }
                
                if (j == 0)
                {
                    if (ch < strings[j].charAt(index))
                    {
                        String[] left = newStrings(string);
                        System.arraycopy(splits, i, splits, i + 1, SPLITS_LENGTH - (i + 1));
                        splits[i] = new Split(ch, index, left);
                    }
                    else
                    {
                        throw new UnsupportedOperationException();
                    }
                }
                else if (j == strings.length)
                {
                    char test = strings[strings.length - 1].charAt(index);
                    Split left = new Split(test, index, split.children);
                    Split right = new Split(ch, index, newStrings(string));
                    System.arraycopy(splits, i, splits, i + 1, SPLITS_LENGTH - (i + 1));
                    splits[i] = left;
                    splits[i + 1] = right;
//
//                    int ix = index + 1;
//                    int k = 0;
//                    for (;;)
//                    {
//                        if (k == strings.length)
//                        {
//                            throw new UnsupportedOperationException();
//                        }
//                        
//                        if (strings[k].length() == ix)
//                        {
//                            throw new UnsupportedOperationException();
//                        }
//                            
//                        if (string.charAt(ix) <= strings[k].charAt(ix))
//                        {
//                            break;
//                        }
//                        k++;
//                    }
//                    if (k == 0)
//                    {
//                        throw new UnsupportedOperationException();
//                    }
//                    else if (k == strings.length)
//                    {
//                        throw new UnsupportedOperationException();
//                    }
//                    else if (strings[k].charAt(index) >= string.charAt(ix))
//                    {
//                        throw new UnsupportedOperationException();
//                    }
                }
                else if (strings[j].charAt(index) >= ch)
                {
                    char test = strings[j - 1].charAt(index);
                    int at = j;
                    String[] left = newStrings(string);
                    System.arraycopy(strings, 0, left, 0, at);
                    System.arraycopy(strings, at, strings, 0, strings.length - at);
                    Arrays.fill(strings, at + 1, strings.length, null);
                    System.arraycopy(splits, i, splits, i + 1, SPLITS_LENGTH - (i + 1));
                    splits[i] = new Split(test, index, left);
                    hasSlot(strings, string);
                }
                else
                {
                    throw new IllegalStateException();
                }
            }
        }
        else if (splits[i].children instanceof Split[])
        {
            throw new UnsupportedOperationException();
        }
        else 
        {
            throw new IllegalStateException();
        }
    }
    
    private static String[] newStrings(String string)
    {
        String[] strings = newStrings();
        strings[0] = string;
        return strings;
    }

    private static String[] newStrings()
    {
        return new String[STRINGS_LENGTH];
    }
    
    private static boolean isSlot(String[] strings, int i, String string)
    {
        return i < strings.length
            && (strings[i] == null || strings[i].compareTo(string) >= 0);
    }

    private static boolean hasSlot(String[] strings, String string)
    {
        if (strings[strings.length - 1] != null)
        {
            return false;
        }
        int i;
        for (i = 0; !isSlot(strings, i, string); i++)
        {
            // No operations.
        }
        
        if (i < strings.length)
        {
            System.arraycopy(strings, i, strings, i + 1, strings.length - (i + 1));
            strings[i] = string;
            return true;
        }
        
        return false;
    }
    
    private final static boolean hasSplit(Split[] splits, int i)
    {
        return i < SPLITS_LENGTH - 1 && splits[i] != null;
    }
    
    public final static boolean hasString(String[] strings, int i)
    {
        return i < strings.length && strings[i] != null;
    }
    
    private final static boolean scan(String[] strings, String string)
    {
        for (int i = 0; hasString(strings, i); i++)
        {
            int compare = strings[i].compareTo(string);
            if (compare == 0)
            {
                return true;
            }
            else if (compare > 0)
            {
                return false;
            }
        }
        return false;
    }

    private final static boolean contains(Split[] splits, String string)
    {
        int i = 0;
        int index = 0;
        for (;;)
        {
            char ch = string.charAt(index);
            Split split = splits[i];
            if (ch == split.character)
            {
                if (split.children instanceof String[])
                {
                    return scan((String[]) split.children, string);
                }
            }
            else if (ch < splits[i].character)
            {
                if (split.children instanceof String[])
                {
                    return scan((String[]) split.children, string);
                }
            }
            else if (i == SPLITS_LENGTH - 1 || splits[i + 1] == null)
            {
                if (ch < split.character)
                {
                    if (split.children instanceof String[])
                    {
                        return scan((String[]) split.children, string);
                    }
                }
                else
                {
                    split = splits[SPLITS_LENGTH - 1];
                    if (split.children instanceof String[])
                    {
                        return scan((String[]) split.children, string);
                    }
                }
            }
            else if (ch > splits[i + 1].character)
            {
                
            }
            i++;
        }
    }
    
    private final static Split[] newSplits()
    {
        Split[] splits = new Split[SPLITS_LENGTH];
        splits[0] = new Split(Character.MAX_VALUE, 0, newStrings());
        return splits;
    }
    
    public void testInsertFirstString()
    {
       Split[] splits = newSplits();
       insert(splits, "bad");
       assertTrue(contains(splits, "bad"));
    }
    
    public void testInsertTwoStringsInOrder()
    {
        Split[] splits = newSplits();
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
    }

    public void testInsertTwoStringsOutOfOrder()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
    }

    public void testInsertThreeStringsInOrder()
    {
        Split[] splits = newSplits();
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
        insert(splits, "bed");
        assertTrue(contains(splits, "bed"));
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        assertTrue(contains(splits, "bed"));
        assertTrue(contains(splits, "bad"));
    }

    public void testInsertThreeStringsOutOfOrder()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
        insert(splits, "bed");
        assertTrue(contains(splits, "bed"));
        assertTrue(contains(splits, "bid"));
        assertTrue(contains(splits, "bad"));
    }

    public void testInsertOnRightMostPage()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "cat");
        assertTrue(contains(splits, "cat"));
        assertTrue(contains(splits, "bid"));
    }

    public void testInsertOnPreviousPage()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "act");
        assertTrue(contains(splits, "act"));
        assertTrue(contains(splits, "bid"));
    }
    
    public void testInsertSplitPageBefore()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
        insert(splits, "bed");
        assertTrue(contains(splits, "bed"));
        insert(splits, "act");
        assertTrue(contains(splits, "act"));
        assertTrue(contains(splits, "bed"));
        assertTrue(contains(splits, "bid"));
        assertTrue(contains(splits, "bad"));
    }

    public void testInsertSplitLeftPageLessThan()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "act");
        assertTrue(contains(splits, "act"));
        assertTrue(contains(splits, "bid"));
    }
    
    public void testInsertSplitCurrentPage()
    {
        Split[] splits = newSplits();
        insert(splits, "bid");
        assertTrue(contains(splits, "bid"));
        insert(splits, "act");
        assertTrue(contains(splits, "act"));
        insert(splits, "bad");
        assertTrue(contains(splits, "bad"));
        insert(splits, "bed");
        assertTrue(contains(splits, "bed"));
        assertTrue(contains(splits, "act"));
        assertTrue(contains(splits, "bid"));
        assertTrue(contains(splits, "bad"));
    }
    
    public void insertTest(Split[] splits, String string)
    {
        insert(splits, string);
        assertTrue(contains(splits, string));
    }
    
    public void insertTest(Split[] splits, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            insertTest(splits, strings[i]);
        }
    }

    public void testInsertSplitCurrentPageGreaterThan()
    {
        Split[] splits = newSplits();

        insertTest(splits, "cat");
        
        insertTest(splits, "act");
        
        insertTest(splits, "cam");
        
        insertTest(splits, "bed");
        
        assertTrue(contains(splits, "act"));
        assertTrue(contains(splits, "cam"));
        assertTrue(contains(splits, "cat"));
    }
    
    private void containsTest(Split[] splits, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            assertTrue(contains(splits, strings[i]));
        }
    }
    
    public void testInsertSplitGreaterThan()
    {
        Split[] splits = newSplits();
        
        String[] inserts = { "act", "bid", "bad", "bed" };
        insertTest(splits, inserts);

        insertTest(splits, "cam");
        
        containsTest(splits, inserts);
    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */