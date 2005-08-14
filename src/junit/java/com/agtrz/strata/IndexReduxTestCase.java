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
            throw new UnsupportedOperationException();
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
                throw new UnsupportedOperationException();
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    private static boolean contains(String[] strings, String string)
    {
        for (int i = 0; i < strings.length; i++)
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
            if (i + 1 != tiers.length && tiers[i + 1] != null)
            {
                throw new UnsupportedOperationException();
//                int index = tiers[i].index;
//                char ch = string.charAt(index);
//                if (ch < tiers[i].ch)
//                {
//                    i++;
//                }
            }
            else if (tiers[i].children instanceof String[])
            {
                return contains((String[]) tiers[i].children, string);
            }
        }
    }
    
    private final static int TIERS_LENGTH = 3;
    
    private final static int STRINGS_LENGTH = 3;
    
    private String[] newStrings()
    {
        return new String[STRINGS_LENGTH];
    }

    private Tier[] newFirstTier()
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
}


/* vim: set et sw=4 ts=4 ai tw=72: */
