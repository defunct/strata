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
        insert(splits, string, 0);
    }
    
    private final static void insert(Split[] splits, int at, Split split)
    {
        System.arraycopy(splits, at, splits, at + 1, splits.length - (at + 1));
        splits[at] = split;
    }
    
    private final static void insert(Split[] splits, String string, int index)
    {
        char ch = string.charAt(index);
        int i;
        for (i = 0; hasSplit(splits, i); i++)
        {
            Split split = splits[i];
            if (ch <= split.character)
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
                    if (splits[splits.length - 1] == null)
                    {
                        if (ch < strings[j].charAt(index))
                        {
                            String[] left = newStrings(string);
                            System.arraycopy(splits, i, splits, i + 1, SPLITS_LENGTH - (i + 1));
                            splits[i] = new Split(ch, index, left);
                        }
                        else
                        {
                            j++;
                            for (;;)
                            {
                                if (j == strings.length)
                                {
                                    break;
                                }
                                if (ch < strings[j].charAt(index))
                                {
                                    break;
                                }
                                j++;
                            }

                            if (j == strings.length)
                            {
                                index++;
                                int k = strings.length / 2;
                                if (index >= strings[k].length())
                                {
                                    throw new UnsupportedOperationException();
                                }
                                char middle = strings[k].charAt(index);
                                for (;;)
                                {
                                    if (k < 0)
                                    {
                                        throw new UnsupportedOperationException();
                                    }
                                    if (middle != strings[k - 1].charAt(index))
                                    {
                                        break;
                                    }
                                    k--;
                                }
                                if (k == -1)
                                {
                                    for (;;)
                                    {
                                        k++;
                                        if (k == strings.length)
                                        {
                                            throw new UnsupportedOperationException();
                                        }
                                        if (middle != strings[k].charAt(index))
                                        {
                                            throw new UnsupportedOperationException();
                                        }
                                    }
                                }
                                if (k < strings.length)
                                {
                                    char test = strings[k].charAt(index);
                                    String[] right = newStrings(strings, k);
                                    String[] left = strings;
                                    splits[i] = new Split(split.character, split.index, left);
                                    insert(splits, i + 1, new Split(test, index, right));
                                    if (test < string.charAt(index))
                                    {
                                        hasSlot(left, string);
                                    }
                                    else
                                    {
                                        throw new UnsupportedOperationException();
                                    }
                                }
                            }
                            else
                            {
                                Split right = new Split(splits[i].character,
                                                        index,
                                                        newStrings(strings, j));
                                
                                Split left = new Split(ch, index, split.children);
                                insert(splits, i + 1, right);
                                splits[i] = left;
                                
                                hasSlot((String[]) left.children, string);
                            }
                        }
                    }
                    else
                    {
                        int k = 0;
                        for (;;)
                        {
                            if (k == strings.length)
                            {
                                throw new UnsupportedOperationException();
                            }
                            if (ch <= strings[k].charAt(index))
                            {
                                break;
                            }
                            k++;
                        }
                        
                        if (k == 0)
                        {
                            if (ch < strings[k].charAt(index))
                            {
                                Split[] subSplits = newSubSplits(strings);
                                subSplits[0] = new Split(ch, index, newStrings(string));
                                splits[i] = new Split(split.character, split.index, subSplits);
                            }
                            else
                            {
                                k++;
                                for (;;)
                                {
                                    if (k == strings.length)
                                    {
                                        throw new UnsupportedOperationException();
                                    }
                                    if (ch < strings[k].charAt(index))
                                    {
                                        break;
                                    }
                                    k++;
                                }
                                
                                String[] right = newStrings(strings, k);
                                Split[] subSplits = newSubSplits(right);
                                subSplits[0] = new Split(ch, index, strings);
                                splits[i] = new Split(split.character, split.index, subSplits);
                                hasSlot(strings, string);
                            }
                        }
                        else
                        {
                            throw new IllegalStateException();
                        }
                    }
                }
                else if (j == strings.length)
                {
                    if (splits[splits.length - 1] == null)
                    {
                        char test = strings[strings.length - 1].charAt(index);
                        if (splits[i + 1] == null)
                        {
                            Split left = new Split(test, index, split.children);
                            Split right =  new Split(Character.MAX_VALUE, 0, newStrings(string));
                            splits[i] = left;
                            splits[i + 1] = right;
                        }
                        else
                        {
                            throw new IllegalStateException();
                        }
                    }
                    else
                    {
                        char test = strings[strings.length - 1].charAt(index);
                        String[] left = strings;
                        String[] right = newStrings(string);
                        Split[] subSplits = newSubSplits(right);
                        subSplits[0] = new Split(test, index, left);
                        splits[i] = new Split(split.character, split.index, subSplits);
                    }
                }
                else if (strings[j].charAt(index) >= ch)
                {
                    if (splits[splits.length - 1] == null)
                    {
                        char test = strings[j - 1].charAt(index);
                        String[] right = newStrings(strings, j);
                        String[] left = strings;
                        insert(splits, i, new Split(test, index, left));
                        splits[i + 1] = new Split(split.character, split.index, right);
                        hasSlot(right, string);
                    }
                    else
                    {
                        char test = strings[j - 1].charAt(index);
                        String[] right = newStrings(strings, j);
                        String[] left = strings;
                        Split[] subSplits = newSubSplits(right);
                        subSplits[0] = new Split(test, index, left);
                        splits[i] = new Split(split.character, split.index, subSplits);
                        hasSlot(right, string);
                    }
                }
                else
                {
                    throw new IllegalStateException();
                }
            }
        }
        else if (splits[i].children instanceof Split[])
        {
            Split[] subSplits = (Split[]) splits[i].children;
            for (int j = 0; hasSplit(subSplits, j); j++)
            {
                Split subSplit = subSplits[j];
                if (ch <= subSplit.character)
                {
                    break;
                }
            }
            insert(subSplits, string, index);
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
    
    private static String[] newStrings(String[] strings, int i)
    {
        String[] newStrings = newStrings();
        System.arraycopy(strings, i, newStrings, 0, strings.length - i);
        System.arraycopy(strings, 0, strings, i, strings.length - i);
        Arrays.fill(strings, i, strings.length, null);
        return newStrings;
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
    
    private final static boolean hasString(String[] strings, int i)
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
            if (ch <= split.character)
            {
                if (split.children instanceof String[])
                {
                    return scan((String[]) split.children, string);
                }
                else
                {
                    return contains((Split[]) split.children, string);
                }
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
    
    private final static Split[] newSubSplits(String[] strings)
    {
        Split[] splits = new Split[SPLITS_LENGTH];
        splits[1] = new Split(Character.MAX_VALUE, 0, strings);
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
    
    private void insertTest(Split[] splits, String string)
    {
        insert(splits, string);
        assertTrue(contains(splits, string));
    }
    
    private void insertTest(Split[] splits, String[] strings)
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

    private void alphaTest(Split[] splits, int start, int stop, int direction)
    {
        for (int i = start; i != stop; i += direction)
        {
            insertTest(splits, ALPHABET[i].substring(0, 1));
        }

        for (int i = start; i != stop; i += direction)
        {
            assertTrue(contains(splits, ALPHABET[i].substring(0, 1)));
        }
    }

    public void testInsertAlphaOneLevel()
    {
        Split[] splits = newSplits();
        
        alphaTest(splits, 0, 9 , 1);
    }

    public void testSplitMidPageLastPage()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "cat", "act", "can" };
        insertTest(splits, inserts);
        
        insertTest(splits, "add");
        
        containsTest(splits, inserts);
    }
    
    public void testSplitMidPageBeforeLastPage()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "cat", "act", "bid" };
        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "add");
        
        containsTest(splits, inserts);
    }
    
    public void testSplitFirstTierBefore()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "bed", "bad", "bid", "cat", "can", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "act");
        
        containsTest(splits, inserts);
    }

    public void testSplitFirstTierSplitLeaf()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "bed", "act", "bid", "cat", "can", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "add");
        
        containsTest(splits, inserts);
    }

    public void testSplitLastTier()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "tin", "ton", "tan", "cat", "can", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "zip");
        
        containsTest(splits, inserts);
    }

    public void testSplitFirstTierInLeaf()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "tin", "ton", "tan", "cat", "act", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "bed");
        
        containsTest(splits, inserts);
    }

    public void testSplitSecondTier()
    {
        Split[] splits = newSplits();
     
        alphaTest(splits, 0, 11, 1);
    }

    public void testInsertAlphabet()
    {
        Split[] splits = newSplits();
     
        alphaTest(splits, 0, 25, 1);
    }

    public void testIssueNewStringsSplitWrongLength()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "bed", "act", "ant", "cat", "can", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "add");
        
        containsTest(splits, inserts);
    }

    public void testSplitFirstTierAfterX()
    {
        Split[] splits = newSplits();
        
        String[] inserts = new String[] { "man", "mad", "mid", "act", "bad", "bid", "cat", "can", "cam" };

        insertTest(splits, inserts);
        
        containsTest(splits, inserts);
        
        insertTest(splits, "add");
        
        containsTest(splits, inserts);
    }
    
//    public void testBed()
//    {
//        Split[] splits = newSplits();
//        
//        String[] inserts = new String[] { "ram", "run", "ran", "mid", "mad", "mud" };
//
//        insertTest(splits, inserts);
//        
//        containsTest(splits, inserts);
//        
//        insertTest(splits, "med");
//        
//        containsTest(splits, inserts);
//    }
}


/* vim: set et sw=4 ts=4 ai tw=70: */