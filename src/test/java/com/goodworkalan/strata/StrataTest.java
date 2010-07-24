/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.goodworkalan.stash.Stash;

// TODO Document.
public class StrataTest {
    // TODO Document.
    private Query<Integer> newTransaction() {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ComparableFactory<Integer>() {
            public Comparable<? super Integer> newComparable(Stash stash, Integer object) {
                return object;
            }
        });
        IntegerTier address = schema.create(new Stash(), new IntegerTierStorage());
        Strata<Integer> strata = schema.open(new Stash(), address, new IntegerTierStorage());
        return strata.query();
    }

    // TODO Document.
    @Test
    public void create() {
        Schema<Integer> schema = new Schema<Integer>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new ComparableFactory<Integer>() {
            public Comparable<? super Integer> newComparable(Stash stash, Integer object) {
                return object;
            }
        });
        IntegerTier address = schema.create(new Stash(), new IntegerTierStorage());
        Strata<Integer> strata = schema.open(new Stash(), address, new IntegerTierStorage());
        Query<Integer> query = strata.query();
        query.add(1);
        Cursor<Integer> cursor = query.find(1);
        assertTrue(cursor.hasNext());
        assertEquals((int) cursor.next(), 1);
        assertFalse(cursor.hasNext());
    }
    
    // TODO Document.
    @Test
    public void removeSingle() {
        Query<Integer> transaction = newTransaction();
        transaction.add(1);
        assertEquals((int) transaction.remove(1), 1);
        assertFalse(transaction.find(1).hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */