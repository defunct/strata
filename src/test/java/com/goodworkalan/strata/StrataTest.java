/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;

import org.testng.annotations.Test;

import com.goodworkalan.stash.Stash;

/**
 * Unit tests for the {@link Strata} class.
 *
 * @author Alan Gutierrez
 */
public class StrataTest {
    /** Create a new strata. */
    private Stratagem newTransaction(int inner, int leaf) {
        Schema<Character> schema = new Schema<Character>();
        schema.setInnerCapacity(inner);
        schema.setLeafCapacity(leaf);
        schema.setComparableFactory(new CastComparableFactory<Character>());
        CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
        return new Stratagem(address, schema.open(new Stash(), address, new CharacterTierStorage()));
    }
    
    public static class Stratagem {
        public final CharacterTier address;
        
        public final Strata<Character> strata;

        public Stratagem(CharacterTier address, Strata<Character> strata) {
            this.address = address;
            this.strata = strata;
        }

        public void assertTree(String inserts, String tree) {
            boolean inserting = true;
            for (int i = 0, stop = inserts.length(); i < stop; i++) {
               char ch = inserts.charAt(i);
               if (ch == '/') {
                   inserting = !inserting;
               } else if (inserting) {
                   strata.query().add(ch);
               } else {
                   strata.query().remove(ch);
               }
            }
            LinkedList<Element> stack = new LinkedList<Element>();
            stack.addLast(new Element(address));
            BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(tree + ".tree")));
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    int indent = 0;
                    while (indent < line.length() && line.charAt(indent) == ' ') {
                        indent++;
                    }
                    char ch = line.charAt(indent);
                    indent /= 2;
                    if (indent < stack.size() - 1) {
                        while (stack.size() - 1 > indent) {
                            Element element = stack.getLast();
                            assertEquals(element.index, element.tier.getSize());
                            stack.removeLast();
                        }
                    } else if (indent > stack.size() - 1) {
                        Element element = stack.getLast();
                        stack.addLast(new Element(element.tier.getChildAddress(element.index)));
                    }
                    Element element = stack.getLast();
                    char record = element.tier.getRecord(element.index) == null ? '<' : ch;
                    element.index++;
                    assertEquals(record, ch);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    public static class Element {
        public int index;
        public CharacterTier tier;
        public Element(CharacterTier tier) {
            this.tier = tier;
        }
    }
    
    /** Create a new strata. */
    private Stratagem newTransaction() {
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
        Query<Character> transaction = newTransaction().strata.query();
        transaction.add('a');
        assertEquals((char) transaction.remove('a'), 'a');
        assertFalse(transaction.find('a').hasNext());
    }
    
    /** Subsequent release of cursor is a no-op. */
    @Test
    public void cursorRelease() {
        Query<Character> query = newTransaction().strata.query();
        query.add('a');
        Cursor<Character> cursor = query.find('a');
        cursor.release();
        cursor.release();
    }
    
    /** Remove is an unsupported operation. */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void cursorRemove() {
        newTransaction().strata.query().find('a').remove();
    }
    
    /** Split leaf tier. */
    @Test
    public void splitLeaf() {
        Stratagem stratagem = newTransaction();
        stratagem.assertTree("a", "addSingle");
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */