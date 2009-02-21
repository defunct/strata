/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.tuple.partial.Compare;
import com.goodworkalan.tuple.partial.Partial;
import com.mallardsoft.tuple.Pair;
import com.mallardsoft.tuple.Single;
import com.mallardsoft.tuple.Tuple;


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
        return schema.create(new Stash(), new InMemoryStorageBuilder<Integer, Integer>());
    }

    @Test public void create()
    {
        Schema<Integer, Integer> schema = Stratas.<Integer, Integer>newInMemorySchema();
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
        Query<Integer, Integer> transaction = schema.create(new Stash(), new InMemoryStorageBuilder<Integer, Integer>());
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
    
    public void kissPerson(Person person)
    {
        // Do some kissing here...
    }
    
    @Test
    public void tuple()
    {
        Schema<Person, Pair<String, String>> schema = Stratas.newInMemorySchema();
        schema.setInnerSize(5);
        schema.setLeafSize(7);
        schema.setExtractor(new Extractor<Person, Pair<String, String>>()
        {
            public Pair<String, String> extract(Stash stash, Person person)
            {
                return Tuple.from(person.getLastName(), person.getFirstName());
            }
        });
        Query<Person, Pair<String, String>> query = schema.create(new Stash(), new InMemoryStorageBuilder<Person, Pair<String, String>>());
        
        Person person = new Person();
        person.setFirstName("Thomas");
        person.setLastName("Jefferson");
        query.add(person);
        
        person = new Person();
        person.setFirstName("George");
        person.setLastName("Jefferson");
        query.add(person);
        
        person = new Person();
        person.setFirstName("Don");
        person.setLastName("Johnson");
        query.add(person);
        
        person = new Person();
        person.setFirstName("Henry");
        person.setLastName("James");
        query.add(person);
        
        Partial<Pair<String, String>, Single<String>> byLastName = Compare.oneOf(Compare.<String, String>pair()); 
        
        Cursor<Person> cursor = query.find(byLastName.compare(Tuple.from("Johnson")));
        assertTrue(cursor.hasNext());
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */