/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;


public class StrataTestCase
{
    private Query<Integer> newTransaction()
    {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        Ilk.Pair address = schema.create(new Stash(), new InMemoryStorage<Integer>(new Ilk<Integer>() { }));
        return schema.open(new Stash(), address, new InMemoryStorage<Integer>(new Ilk<Integer>() { })).query();
    }

    @Test public void create()
    {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        Query<Integer> query = schema.inMemory(new Stash(), new Ilk<Integer>() { }).query();
        query.add(1);
        Cursor<Integer> cursor = query.find(1);
        assertTrue(cursor.hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
    }
    
    @Test public void removeSingle()
    {
        Query<Integer> transaction = newTransaction();
        transaction.add(1);
        assertEquals((int) transaction.remove(1), 1);
        assertFalse(transaction.find(1).hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */