package com.agtrz.strata;

import java.util.Collection;
import java.util.Iterator;

import junit.framework.TestCase;

public class StrataTestCase
extends TestCase
{
    private final static String[] ALPHABET = new String[] { "alpha", "beta", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whisky", "x-ray", "zebra" };

    public static class Employee
    {
        public final long employeeId;

        public final String firstName;

        public final String lastName;

        public Employee(long employeeId, String firstName, String lastName)
        {
            this.employeeId = employeeId;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }

    public void testConstruction()
    {
        Strata strata = new Strata();
        strata.insert(ALPHABET[0]);
        assertOneEquals(ALPHABET[0], strata.find(ALPHABET[0]));
    }

    public void testMultiple()
    {
        Strata strata = new Strata();
        for (int i = 0; i < 8; i++)
        {
            strata.insert(ALPHABET[i]);
            assertOneEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        for (int i = 0; i < 8; i++)
        {
            assertOneEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        strata.copacetic();
    }

    private static void assertOneEquals(Object object, Collection collection)
    {
        Iterator iterator = collection.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(object.toString(), iterator.next().toString());
        assertFalse(iterator.hasNext());
    }

    private static void assertEquals(int count, Object object, Collection collection)
    {
        Iterator iterator = collection.iterator();
        for (int i = 0; i < count; i++)
        {
            assertTrue(iterator.hasNext());
            assertEquals(object.toString(), iterator.next().toString());
        }
        assertFalse(iterator.hasNext());
    }

    private void assertInsert(Strata strata, int[] insert)
    {
        for (int i = 0; i < insert.length; i++)
        {
            strata.insert(new Integer(insert[i]));
            strata.copacetic();
        }
    }

    private void assertContains(Strata strata, int[] contents)
    {

    }

    public void testSplit()
    {
        Strata strata = new Strata();
        for (int i = 0; i < 9; i++)
        {
            strata.insert(ALPHABET[i]);
            assertOneEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        for (int i = 0; i < 9; i++)
        {
            try
            {
                assertOneEquals(ALPHABET[i], strata.find(ALPHABET[i]));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + ALPHABET[i], e);
            }
        }
    }

    public void testSplitRoot()
    {
        Strata strata = new Strata();
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                strata.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertOneEquals(insert, strata.find(insert));
            strata.copacetic();
        }
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                assertOneEquals(insert, strata.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            strata.copacetic();
        }
    }

    public void testSplitRootPseudoRandom()
    {
        Strata strata = new Strata();
        int hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                strata.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertOneEquals(insert, strata.find(insert));
            strata.copacetic();
        }
        hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                assertOneEquals(insert, strata.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            strata.copacetic();
        }
    }

    public void testDuplicate()
    {
        Strata strata = new Strata();
        strata.insert(new Integer(2));
        strata.copacetic();
        strata.insert(new Integer(2));
        strata.copacetic();
        assertEquals(2, new Integer(2), strata.find(new Integer(2)));
    }

    public void testUnsplittable()
    {
        Strata strata = new Strata();
        for (int i = 0; i < 5; i++)
        {
            strata.insert(new Integer(2));
            strata.copacetic();
        }
        strata.insert(new Integer(2));
        strata.copacetic();
        assertEquals(6, new Integer(2), strata.find(new Integer(2)));
    }

    public void testTwoUnsplittables()
    {
        Strata strata = new Strata();
        for (int i = 0; i < 10; i++)
        {
            strata.insert(new Integer(2));
            strata.copacetic();
        }
        strata.insert(new Integer(2));
        strata.copacetic();
        assertEquals(11, new Integer(2), strata.find(new Integer(2)));
    }

    public void testDuplicatesInCenter()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(strata, insert);

        strata.insert(new Integer(2));
        strata.copacetic();

        assertEquals(1, new Integer(1), strata.find(new Integer(1)));
        assertEquals(4, new Integer(2), strata.find(new Integer(2)));
        assertEquals(1, new Integer(3), strata.find(new Integer(3)));
    }

    public void testDuplicatesLeftOfCenter()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(strata, insert);

        strata.insert(new Integer(1));
        strata.copacetic();

        assertEquals(2, new Integer(1), strata.find(new Integer(1)));
        assertEquals(3, new Integer(2), strata.find(new Integer(2)));
        assertEquals(1, new Integer(3), strata.find(new Integer(3)));
    }

    public void testDuplicatesRightOfCenter()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(strata, insert);

        strata.insert(new Integer(3));
        strata.copacetic();

        assertEquals(1, new Integer(1), strata.find(new Integer(1)));
        assertEquals(3, new Integer(2), strata.find(new Integer(2)));
        assertEquals(2, new Integer(3), strata.find(new Integer(3)));
    }

    public void testUnsplittableRight()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 2, 2, 2, 2, 2 };

        assertInsert(strata, insert);

        strata.insert(new Integer(3));
        strata.copacetic();

        assertEquals(5, new Integer(2), strata.find(new Integer(2)));
        assertEquals(1, new Integer(3), strata.find(new Integer(3)));
    }

    public void testLinkedUnsplittableRight()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 2, 2, 2, 2, 2, 2 };

        assertInsert(strata, insert);

        strata.insert(new Integer(3));
        strata.copacetic();

        assertEquals(6, new Integer(2), strata.find(new Integer(2)));
        assertEquals(1, new Integer(3), strata.find(new Integer(3)));
    }

    public void testUnsplittableLeft()
    {
        Strata strata = new Strata();
        int[] insert = new int[] { 2, 2, 2, 2, 2 };

        assertInsert(strata, insert);

        strata.insert(new Integer(1));
        strata.copacetic();

        assertEquals(1, new Integer(1), strata.find(new Integer(1)));
        assertEquals(5, new Integer(2), strata.find(new Integer(2)));
    }

    public void testTraverse()
    {
        Strata strata = new Strata();

        int[] insert = new int[] { 1, 2, 3, 4, 5, 6, 7, 7, 7, 8, 9 };
        assertInsert(strata, insert);

        Iterator iterator = strata.values().iterator();
        for (int i = 0; i < insert.length; i++)
        {
            assertTrue(iterator.hasNext());
            Integer integer = (Integer) iterator.next();
            assertEquals(insert[i], integer.intValue());
        }
        assertFalse(iterator.hasNext());
    }

    public void testRemove()
    {
        Strata strata = new Strata();

        int[] insert = new int[] { 1, 2, 3, 4, 5 };
        assertInsert(strata, insert);

        assertRemove(strata, 3, 1);
        assertContains(strata, new int[] { 1, 2, 4, 5 });
    }

    public void testRemoveMany()
    {
        Strata strata = new Strata();

        int[] insert = new int[] { 1, 3, 5, 3, 3 };
        assertInsert(strata, insert);

        assertRemove(strata, 3, 3);
        assertContains(strata, new int[] { 1, 5 });

    }

    public void testRemoveSplit()
    {
        Strata strata = new Strata();

        int[] insert = new int[] { 2, 3, 4, 5, 6, 1 };
        assertInsert(strata, insert);

        assertRemove(strata, 3, 1);
        assertContains(strata, new int[] { 1, 2, 3, 4, 5 });
    }

    public void testRemoveSplitRightMost()
    {
        Strata strata = new Strata();

        int[] insert = new int[] { 2, 3, 4, 5, 6, 1 };
        assertInsert(strata, insert);

        assertRemove(strata, 6, 1);
        assertContains(strata, new int[] { 1, 2, 3, 4, 5 });
    }

    private void assertRemove(Strata strata, int value, int count)
    {
        Iterator iterator = strata.remove(new Integer(value)).iterator();
        for (int i = 0; i < count; i++)
        {
            assertTrue(iterator.hasNext());
            Integer integer = (Integer) iterator.next();
            assertEquals(value, integer.intValue());
        }
        assertFalse(iterator.hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */