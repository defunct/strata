/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import java.util.Arrays;

import junit.framework.TestCase;

/**
 * @author Alan Gutierez
 */
public class IndexReduxTestCase
extends TestCase
{
    private final static class Tier
    {
        public final char ch;
        public final int index;
        public final Object[] children;
        
        public Tier(char character, int index, Object[] children)
        {
            this.ch = character;
            this.index = index;
            this.children = children;
        }
    }
    
    private static void insertTest(Tier[] tiers, String string)
    {
        insert(tiers, string);
        assertTrue(contains(tiers, string));
    }
    
    private static void insertTest(Tier[] tiers, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            insertTest(tiers, strings[i]);
        }
        
        containsTest(tiers, strings);
    }
    
    private static void containsTest(Tier[] tiers, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            assertTrue(contains(tiers, strings[i]));
        }
    }
    
    private static boolean hasSlot(Object[] slots) 
    {
        return slots[slots.length - 1] == null;
    }

    private static void insert(String[] strings, String string)
    {
        if (!hasSlot(strings))
        {
            throw new IllegalArgumentException();
        }
        
        int i;
        for (i = 0; strings[i] != null && strings[i].compareTo(string) < 0; i++)
        {
            // No operation.
        }
        
        System.arraycopy(strings, i, strings, i + 1, strings.length - (i + 1));
        strings[i] = string;
    }
    
    private final static int LEFT = 0;
    private final static int RIGHT = 1;
    
    private static String[][] split(String[] strings, int at)
    {
        String[] left = strings;
        String[] right = newStrings();
        System.arraycopy(strings, at, right, 0, strings.length - at);
        System.arraycopy(strings, 0, left, at, strings.length - at);
        Arrays.fill(left, at, strings.length, null);
        return new String[][] { left, right };
    }
    
    private static void insert(Tier[] tiers, int at, Tier tier)
    {
        System.arraycopy(tiers, at, tiers, at + 1, tiers.length - (at + 1));
        tiers[at] = tier;
    }

    private static void insert(Tier[] tiers, String string)
    {
        int i = 0;
        int index = 0;
        for (;;)
        {
            if (i + 1 == tiers.length)
            {
                throw new UnsupportedOperationException();
            }
            if (tiers[i + 1] == null)
            {
                break;
            }
            if (tiers[i + 1].ch > string.charAt(tiers[i + 1].index))
            {
                break;
            }
            i++;
        }
        if (i == tiers.length)
        {
            throw new IllegalStateException();
        }
        
        Tier tier = tiers[i];
        if (tier.children instanceof String[])
        {
            String[] strings = (String[]) tier.children;
            if (hasSlot(strings))
            {
                insert(strings, string);
            }
            else
            {
                int j = strings.length / 2;
                char middle = strings[j].charAt(index);
                for (;;)
                {
                    if (j == 0)
                    {
                        break;
                    }
                    if (middle != strings[j - 1].charAt(index))
                    {
                        break;
                    }
                    j--;
                }
                if (j == 0)
                {
                    for (;;)
                    {
                        j++;
                        if (j == strings.length)
                        {
                            throw new UnsupportedOperationException();
                        }
                        if (middle != strings[j].charAt(index))
                        {
                            break;
                        }
                    }
                }
                
                if (j == strings.length)
                {
                    throw new UnsupportedOperationException();
                }
                else
                {
                    String[][] split = split(strings, j);
                    insert(tiers, i + 1, new Tier(middle, index, split[RIGHT]));
                    tiers[i] = new Tier(tier.ch, tier.index, split[LEFT]);
                    insert(tiers, string);
                }
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    private static boolean contains(String[] strings, String string)
    {
        for (int i = 0; i < strings.length && strings[i] != null; i++)
        {
            if (strings[i].equals(string))
            {
                return true;
            }
        }
        return false;
    }
 
    private static boolean contains(Tier[] tiers, String string)
    {
        int i = 0;
        for (;;)
        {
            if (i + 1 == tiers.length)
            {
                throw new UnsupportedOperationException();
            }
            if (tiers[i + 1] == null)
            {
                break;
            }
            if (tiers[i + 1].ch > string.charAt(tiers[i + 1].index))
            {
                break;
            }
            i++;
        }
        if (tiers[i].children instanceof String[])
        {
            return contains((String[]) tiers[i].children, string);
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    private final static int TIERS_LENGTH = 3;
    
    private final static int STRINGS_LENGTH = 3;
    
    private static String[] newStrings()
    {
        return new String[STRINGS_LENGTH];
    }

    private static Tier[] newFirstTier()
    {
        Tier[] tiers = new Tier[TIERS_LENGTH];
        tiers[0] = new Tier(Character.MIN_VALUE, 0, newStrings());
        return tiers;
    }
    
    public void testInsertFirstString()
    {
       Tier[] tiers = newFirstTier();
       insert(tiers, "bad");
       assertTrue(contains(tiers, "bad"));
    }


    public void testInsertTwoStringsInOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bad");
        insertTest(tiers, "bid");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
    }

    public void testInsertTwoStringsOutOfOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bid");
        insertTest(tiers, "bad");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
    }

    public void testInsertThreeStringsInOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bad");
        insertTest(tiers, "bed");
        insertTest(tiers, "bid");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bed"));
        assertTrue(contains(tiers, "bad"));
    }

    public void testInsertThreeStringsOutOfOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bid");
        insertTest(tiers, "bad");
        insertTest(tiers, "bed");
        assertTrue(contains(tiers, "bed"));
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
    }

    public void testInsertSplitPage()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "act", "bad", "cat" });
        insertTest(tiers, "car");
    }

    public void testInsertSplitPageBeginningWithSought()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "bid", "bad", "cat" });
        insertTest(tiers, "bed");
    }

    public void testInsertOntoFirstPageAfterSplit()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "act", "bad", "cat" });
        insertTest(tiers, "add");
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */
