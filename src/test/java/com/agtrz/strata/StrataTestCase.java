/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

import com.agtrz.strata.Strata.Record;

public class StrataTestCase
{
    @Test public void create()
    {
        Strata.Schema<Integer, Object> schema = Strata.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        Strata.Extractor<Integer, Object> extractor = new Strata.Extractor<Integer, Object>()
        {
            public void extract(Object txn, Integer object, Record record)
            {
                record.fields(object);
            }
        };
        schema.setExtractor(extractor);
        Strata.Transaction<Integer, Object> transaction = schema.newTransaction(null);
        transaction.add(1);
        Strata.Cursor<Integer> cursor = transaction.find(1);
        assertTrue(cursor.hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */