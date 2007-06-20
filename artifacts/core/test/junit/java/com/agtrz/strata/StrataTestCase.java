package com.agtrz.strata;

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
        Strata.Query query = strata.query(null);
        query.insert(ALPHABET[0]);
        assertOneEquals(ALPHABET[0], query.find(ALPHABET[0]));
    }

    public void testSingleRemove()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        query.insert(ALPHABET[0]);
        assertOneEquals(ALPHABET[0], query.find(ALPHABET[0]));
        assertEquals(ALPHABET[0], query.remove(ALPHABET[0]));
    }

    public void testMultiple()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        for (int i = 0; i < 8; i++)
        {
            query.insert(ALPHABET[i]);
            assertOneEquals(ALPHABET[i], query.find(ALPHABET[i]));
            query.copacetic();
        }
        for (int i = 0; i < 8; i++)
        {
            assertOneEquals(ALPHABET[i], query.find(ALPHABET[i]));
        }
        query.copacetic();
    }

    private static void assertOneEquals(Object object, Strata.Cursor cursor)
    {
        assertTrue(cursor.hasNext());
        assertEquals(object.toString(), cursor.next().toString());
    }

    private static void assertEquals(int count, Object object, Strata.Cursor cursor)
    {
        for (int i = 0; i < count; i++)
        {
            assertTrue(cursor.hasNext());
            assertEquals(object.toString(), cursor.next().toString());
        }
    }

    private void assertInsert(Strata.Query query, int[] insert)
    {
        for (int i = 0; i < insert.length; i++)
        {
            query.insert(new Integer(insert[i]));
            query.copacetic();
        }
    }

    private void assertContains(Strata.Query query, int[] contents)
    {

    }

    public void testSplit()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        for (int i = 0; i < 9; i++)
        {
            query.insert(ALPHABET[i]);
            assertOneEquals(ALPHABET[i], query.find(ALPHABET[i]));
        }
        for (int i = 0; i < 9; i++)
        {
            try
            {
                assertOneEquals(ALPHABET[i], query.find(ALPHABET[i]));
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
        Strata.Query query = strata.query(null);
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                query.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertOneEquals(insert, query.find(insert));
            query.copacetic();
        }
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                assertOneEquals(insert, query.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            query.copacetic();
        }
    }

    public void testSplitRootPseudoRandom()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                query.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertOneEquals(insert, query.find(insert));
            query.copacetic();
        }
        hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                assertOneEquals(insert, query.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            query.copacetic();
        }
    }

    public void testDuplicate()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        query.insert(new Integer(2));
        query.copacetic();
        query.insert(new Integer(2));
        query.copacetic();
        assertEquals(2, new Integer(2), query.find(new Integer(2)));
    }

    public void testUnsplittable()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        for (int i = 0; i < 5; i++)
        {
            query.insert(new Integer(2));
            query.copacetic();
        }
        query.insert(new Integer(2));
        query.copacetic();
        assertEquals(6, new Integer(2), query.find(new Integer(2)));
    }

    public void testTwoUnsplittables()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        for (int i = 0; i < 10; i++)
        {
            query.insert(new Integer(2));
            query.copacetic();
        }
        query.insert(new Integer(2));
        query.copacetic();
        assertEquals(11, new Integer(2), query.find(new Integer(2)));
    }

    public void testDuplicatesInCenter()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(query, insert);

        query.insert(new Integer(2));
        query.copacetic();

        assertEquals(1, new Integer(1), query.find(new Integer(1)));
        assertEquals(4, new Integer(2), query.find(new Integer(2)));
        assertEquals(1, new Integer(3), query.find(new Integer(3)));
    }

    public void testDuplicatesLeftOfCenter()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(query, insert);

        query.insert(new Integer(1));
        query.copacetic();

        assertEquals(2, new Integer(1), query.find(new Integer(1)));
        assertEquals(3, new Integer(2), query.find(new Integer(2)));
        assertEquals(1, new Integer(3), query.find(new Integer(3)));
    }

    public void testDuplicatesRightOfCenter()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 1, 2, 2, 2, 3 };

        assertInsert(query, insert);

        query.insert(new Integer(3));
        query.copacetic();

        assertEquals(1, new Integer(1), query.find(new Integer(1)));
        assertEquals(3, new Integer(2), query.find(new Integer(2)));
        assertEquals(2, new Integer(3), query.find(new Integer(3)));
    }

    public void testUnsplittableRight()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 2, 2, 2, 2, 2 };

        assertInsert(query, insert);
        query.copacetic();

        query.insert(new Integer(3));
        query.copacetic();

        assertEquals(5, new Integer(2), query.find(new Integer(2)));
        assertEquals(1, new Integer(3), query.find(new Integer(3)));
    }

    public void testLinkedUnsplittableRight()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 2, 2, 2, 2, 2, 2 };

        assertInsert(query, insert);

        query.insert(new Integer(3));
        query.copacetic();

        assertEquals(6, new Integer(2), query.find(new Integer(2)));
        assertEquals(1, new Integer(3), query.find(new Integer(3)));
    }

    public void testUnsplittableLeft()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 2, 2, 2, 2, 2 };

        assertInsert(query, insert);

        query.insert(new Integer(1));
        query.copacetic();

        assertEquals(1, new Integer(1), query.find(new Integer(1)));
        assertEquals(5, new Integer(2), query.find(new Integer(2)));
    }

    public void testUnsplittableLeftNotFirstEntry()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        int[] insert = new int[] { 5, 5, 5, 5, 5 };

        assertInsert(query, insert);

        query.insert(new Integer(1));
        query.copacetic();

        query.insert(new Integer(3));
        query.copacetic();

        assertEquals(1, new Integer(1), query.find(new Integer(1)));
        assertEquals(5, new Integer(5), query.find(new Integer(5)));
    }

    public void testTraverse()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        int[] insert = new int[] { 1, 2, 3, 4, 5, 6, 7, 7, 7, 8, 9 };
        assertInsert(query, insert);

        Strata.Cursor cursor = query.first();
        for (int i = 0; i < insert.length; i++)
        {
            assertTrue(cursor.hasNext());
            Integer integer = (Integer) cursor.next();
            assertEquals(insert[i], integer.intValue());
        }
        assertFalse(cursor.hasNext());
    }

    public void testRemove()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        int[] insert = new int[] { 1, 2, 3, 4, 5 };
        assertInsert(query, insert);

        assertRemove(query, 3, 1);
        assertContains(query, new int[] { 1, 2, 4, 5 });
    }

    public void testRemoveMany()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        int[] insert = new int[] { 1, 3, 5, 3, 3 };
        assertInsert(query, insert);

        assertRemove(query, 3, 3);
        assertContains(query, new int[] { 1, 5 });
    }

    public void testRemoveSplit()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        int[] insert = new int[] { 2, 3, 4, 5, 6, 1 };
        assertInsert(query, insert);
        query.copacetic();

        assertRemove(query, 3, 1);
        query.copacetic();
        assertContains(query, new int[] { 1, 2, 3, 4, 5 });
    }

    public void testRemoveSplitRightMost()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        int[] insert = new int[] { 2, 3, 4, 5, 6, 1 };
        assertInsert(query, insert);

        assertRemove(query, 6, 1);
        query.copacetic();
        assertContains(query, new int[] { 1, 2, 3, 4, 5 });
    }

    public void testRemoveSwapKey()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        for (int i = 1; i <= 100; i++)
        {
            query.insert(new Integer(i));
            query.copacetic();
        }

        assertRemove(query, 43, 1);
        query.copacetic();
        assertContains(query, new int[] { 1, 2, 3, 5, 6, 7 });
    }

    public void testRemoveMergeRoot()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        for (int i = 1; i <= 45; i++)
        {
            query.insert(new Integer(i));
            query.copacetic();
        }

        Integer fortySix = new Integer(46);
        query.insert(fortySix);
        query.copacetic();

        assertRemove(query, 46, 1);
        query.copacetic();
    }

    public void testRemoveMergeRootAndSwap()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        for (int i = 1; i <= 45; i++)
        {
            query.insert(new Integer(i));
            query.copacetic();
        }

        Integer fortySix = new Integer(46);
        query.insert(fortySix);
        query.copacetic();

        assertOneEquals(new Integer(19), query.find(new Integer(19)));
        assertOneEquals(new Integer(20), query.find(new Integer(20)));
        assertRemove(query, 19, 1);
        assertOneEquals(new Integer(20), query.find(new Integer(20)));
        assertRemove(query, 20, 1);
        query.copacetic();
    }

    public void testRemoveLeftMostSwapNoMerge()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        for (int i = 1; i <= 45; i++)
        {
            query.insert(new Integer(i));
            query.copacetic();
        }

        Integer fortySix = new Integer(46);
        query.insert(fortySix);
        query.copacetic();

        Integer twenty = new Integer(20);
        for (int i = 0; i < 4; i++)
        {
            query.insert(twenty);
            query.copacetic();
        }

        assertRemove(query, 19, 1);
        query.copacetic();
    }

    public void testRemoveShift()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);

        for (int i = 1; i <= 7; i++)
        {
            query.insert(new Integer(i));
            query.copacetic();
        }

        for (int i = 0; i < 15; i++)
        {
            query.insert(new Integer(3));
            query.copacetic();
        }

        assertRemove(query, 3, 16);
        query.copacetic();
    }
    
    public void testRemoveNumbers()
    {
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        
        query.insert(new Integer(1));
        query.insert(new Integer(1));
        query.insert(new Integer(1));
        query.insert(new Integer(1));
        query.insert(new Integer(1));
        
        query.remove(new Integer(1));
    }

    private void assertRemove(Strata.Query query, int value, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Object object = query.remove(new Integer(value));
            assertNotNull(object);
            Integer integer = (Integer) object;
            assertEquals(value, integer.intValue());
        }
        assertNull(query.remove(new Integer(value)));
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */