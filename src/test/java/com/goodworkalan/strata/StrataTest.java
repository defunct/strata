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
    private Strata<Character> newTransaction(int inner, int leaf) {
        Schema<Character> schema = new Schema<Character>();
        schema.setInnerCapacity(inner);
        schema.setLeafCapacity(leaf);
        schema.setComparableFactory(new CastComparableFactory<Character>());
        CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
        return schema.open(new Stash(), address, new CharacterTierStorage());
    }

    /** Create a new strata. */
    private Strata<Character> newTransaction() {
        return newTransaction(4, 4);
    }

    /** Create a Strata. */
    @Test
    public void create() {
        Schema<Character> schema = new Schema<Character>();
        schema.setInnerCapacity(5);
        schema.setLeafCapacity(7);
        schema.setComparableFactory(new CastComparableFactory<Character>());
        CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
        Strata<Character> strata = schema.open(new Stash(), address, new CharacterTierStorage());
        Query<Character> query = strata.query();
        query.add('a');
        Cursor<Character> cursor = query.find('a');
        assertTrue(cursor.hasNext());
        assertTrue(cursor.newCursor().hasNext());
        assertEquals((char) cursor.next(), 'a');
        assertFalse(cursor.hasNext());
        assertFalse(cursor.newCursor().hasNext());
    }
    
    /** Add an item. */
    @Test
    public void add() {
        Query<Character> transaction = newTransaction().query();
        transaction.add('a');
        assertEquals((char) transaction.remove('a'), 'a');
        assertFalse(transaction.find('a').hasNext());
    }
    
    /** Subsequent release of cursor is a no-op. */
    @Test
    public void cursorRelease() {
        Query<Character> query = newTransaction().query();
        query.add('a');
        Cursor<Character> cursor = query.find('a');
        cursor.release();
        cursor.release();
    }
    
    /** Remove is an unsupported operation. */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void cursorRemove() {
        newTransaction().query().find('a').remove();
    }
    
    /** Split leaf tier. */
    @Test
    public void splitLeaf() {
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */