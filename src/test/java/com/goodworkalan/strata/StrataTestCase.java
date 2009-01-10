/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.goodworkalan.favorites.Stash;


public class StrataTestCase
{
    private Query<Integer, Integer> newTransaction()
    {
        Schema<Integer, Integer> schema = Stratas.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Extractor<Integer, Integer> extractor = new Extractor<Integer, Integer>()
        {
            public Integer extract(Stash stash, Integer object)
            {
                return object;
            }
        };
        schema.setExtractor(extractor);
        return schema.newTransaction(null);
    }

    @Test public void create()
    {
        Schema<Integer, Integer> schema = Stratas.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Extractor<Integer, Integer> extractor = new Extractor<Integer, Integer>()
        {
            public Integer extract(Stash stash, Integer object)
            {
                return object;
            }
        };
        schema.setExtractor(extractor);
        Query<Integer, Integer> transaction = schema.newTransaction(null);
        transaction.add(1);
        Cursor<Integer> cursor = transaction.find(1);
        assertTrue(cursor.hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
    }
    
    @Test public void removeSingle()
    {
        Query<Integer, Integer> transaction = newTransaction();
        transaction.add(1);
        assertEquals((int) transaction.remove(1), 1);
        assertFalse(transaction.find(1).hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */