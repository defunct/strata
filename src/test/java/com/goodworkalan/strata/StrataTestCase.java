/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

import com.goodworkalan.strata.Cursor;
import com.goodworkalan.strata.Extractor;
import com.goodworkalan.strata.Record;
import com.goodworkalan.strata.Schema;
import com.goodworkalan.strata.Stratas;
import com.goodworkalan.strata.Transaction;


public class StrataTestCase
{
    private Transaction<Integer, Object> newTransaction()
    {
        Schema<Integer, Object> schema = Stratas.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Extractor<Integer, Object> extractor = new Extractor<Integer, Object>()
        {
            public void extract(Object txn, Integer object, Record record)
            {
                record.fields(object);
            }
        };
        schema.setExtractor(extractor);
        return schema.newTransaction(null);
    }
    @Test public void create()
    {
        Schema<Integer, Object> schema = Stratas.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Extractor<Integer, Object> extractor = new Extractor<Integer, Object>()
        {
            public void extract(Object txn, Integer object, Record record)
            {
                record.fields(object);
            }
        };
        schema.setExtractor(extractor);
        Transaction<Integer, Object> transaction = schema.newTransaction(null);
        transaction.add(1);
        Cursor<Integer> cursor = transaction.find(1);
        assertTrue(cursor.hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
    }
    
    @Test public void removeSingle()
    {
        Transaction<Integer, Object> transaction = newTransaction();
        transaction.add(1);
        assertEquals(transaction.remove(1), 1);
        assertFalse(transaction.find(1).hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */