/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.goodworkalan.stash.Stash;

/**
 * Unit tests for the {@link Strata} class.
 *
 * @author Alan Gutierrez
 */
public class StrataTest {
    /** Create a new strata. */
    private Strata<Integer> newTransaction(int inner, int leaf) {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(inner);
        schema.setLeafCapacity(leaf);
        schema.setComparableFactory(new CastComparableFactory<Integer>());
        IntegerTier address = schema.create(new Stash(), new IntegerTierStorage());
        return schema.open(new Stash(), address, new IntegerTierStorage());
    }

    /** Create a new strata. */
    private Strata<Integer> newTransaction() {
        return newTransaction(4, 4);
    }

    /** Test creation of a strata. */
    @Test
    public void create() {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new CastComparableFactory<Integer>());
        IntegerTier address = schema.create(new Stash(), new IntegerTierStorage());
        Strata<Integer> strata = schema.open(new Stash(), address, new IntegerTierStorage());
        Query<Integer> query = strata.query();
        query.add(1);
        Cursor<Integer> cursor = query.find(1);
        assertTrue(cursor.hasNext());
        assertTrue(cursor.newCursor().hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
        assertFalse(cursor.newCursor().hasNext());
    }
    
    /** Test add. */
    @Test
    public void add() {
        Query<Integer> transaction = newTransaction().query();
        transaction.add(1);
        assertEquals((int) transaction.remove(1), 1);
        assertFalse(transaction.find(1).hasNext());
    }
    
    /** Test double release of cursor. */
    @Test
    public void cursorRelease() {
        Query<Integer> query = newTransaction().query();
        query.add(1);
        Cursor<Integer> cursor = query.find(1);
        cursor.release();
        cursor.release();
    }
    
    /** Test cursor remove. */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void cursorRemove() {
        newTransaction().query().find(1).remove();
    }
    
    /** Create a second tier. */
    @Test
    public void splitRoot() {
        Query<Integer> query = newTransaction().query();
        for (int i = 0; i < 5; i++) {
            query.add(i);   
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */