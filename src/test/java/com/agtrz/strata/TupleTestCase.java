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
        Bag.Extractor<Person, Pair<String, String>> lastNameFirst = new Bag.Extractor<Person, Pair<String,String>>()
        {
            public Pair<String, String> extract(Person person)
            {
                return new Pair<String, String>(person.getLastName(), person.getFirstName());
            }
        };
        Person person = new Person();
        Pair<String, String> fullName = lastNameFirst.extract(person);
        bag.add(person);
        Cursor<Person> people = bag.find(new Pair<String, String>("Gutierrez", null));
        assertEquals("Gutierrez", people.next().getLastName());
        assertEquals("Gutierrez", fullName.getFirst());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */