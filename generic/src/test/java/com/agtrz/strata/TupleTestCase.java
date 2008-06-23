/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import static org.junit.Assert.assertEquals;

import org.junit.Ignore;
import org.junit.Test;

public class TupleTestCase
{
    @Test void pair()
    {
        Pair<String, Integer> pair = new Pair<String, Integer>("Hello", 10);
        assertEquals(0, pair.compareTo(new Pair<String, Integer>("Hello", 10)));
    }
    
    @Ignore @Test void api()
    {
        Bag<Person, Pair<String, String>> bag = null;
        Bag.Extractor<Person, Bag.Two<String, String>> lastNameFirst = new Bag.Extractor<Person, Bag.Two<String, String>>()
        {
            public Bag.Two<String, String> extract(Person person)
            {
                return Bag.box(person.getFirstName(), person.getLastName());
            }
        };
        Person person = new Person();
        Bag.Two<String, String> fullName = lastNameFirst.extract(person);
        bag.add(person);
        Cursor<Person> people = bag.find(new Pair<String, String>("Gutierrez", null));
        assertEquals("Gutierrez", people.next().getLastName());
        assertEquals("Gutierrez", fullName.getFirst());
        Bag.Two<String, String> two = Bag.box("Hello", "World");
        assertEquals(two.getFirst(), "Hello");
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */