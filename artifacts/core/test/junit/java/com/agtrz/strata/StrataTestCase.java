package com.agtrz.strata;

import junit.framework.TestCase;

import com.agtrz.swag.io.ObjectReadBuffer;
import com.agtrz.swag.io.ObjectWriteBuffer;

public class StrataTestCase
extends TestCase 
{
    private final static String[] ALPHABET = new String[]
    {                                        
        "alpha",
        "beta",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whisky",
        "x-ray",
        "zebra"
    };

    public static class Employee
    {
        public final long employeeId;
        
        public final String firstName;
        
        public final String lastName;
        
        public Employee(long employeeId, String firstName, String lastName)
        {
            this.employeeId = employeeId;
            this.firstName = firstName;
            this.lastName = lastName;
        }
    }
    
    public void testConstruction()
    {
        Strata strata = new Strata(new MemoryTierFactory());
        strata.getSchema().addStratifier(String.class, new Stratifier()
        {
            public Object getReference(Object object)
            {
                return new Long(((Employee) object).employeeId);
            }
            
            public Object deserialize(ObjectReadBuffer input)
            {
                return new Employee(input.readLong(),
                                    input.readString(),
                                    input.readString()); 
            }

            public void serialize(ObjectWriteBuffer output, Object object)
            {
                Employee employee = (Employee) object;
                output.write(employee.employeeId);
                output.write(employee.firstName);
                output.write(employee.lastName);
            } 
        });
//        for (int i = 0; i < 1; i++)
//        {
            strata.insert(ALPHABET[0]);
            assertEquals(ALPHABET[0], strata.find(ALPHABET[0]));
//        }
    }
    
    public void testMultiple()
    {
        Strata strata = new Strata(new MemoryTierFactory());
        for (int i = 0; i < 8; i++)
        {
            strata.insert(ALPHABET[i]);
            assertEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        for (int i = 0; i < 8; i++)
        {
            assertEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        strata.copacetic();
    }
    
    public void testSplit()
    {
        Strata strata = new Strata(new MemoryTierFactory());
        for (int i = 0; i < 9; i++)
        {
            strata.insert(ALPHABET[i]);
            assertEquals(ALPHABET[i], strata.find(ALPHABET[i]));
        }
        for (int i = 0; i < 9; i++)
        {
            try
            {
                assertEquals(ALPHABET[i], strata.find(ALPHABET[i]));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + ALPHABET[i], e);
            }
        }
    }

    public void testSplitRoot()
    {
        Strata strata = new Strata(new MemoryTierFactory());
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                strata.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertEquals(insert, strata.find(insert));
            strata.copacetic();
        }
        for (int i = 0; i < 1000; i++)
        {
            Object insert = new Integer(i);
            try
            {
                assertEquals(insert, strata.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            strata.copacetic();
        }
    }

    public void testSplitRootPseudoRandom()
    {
        Strata strata = new Strata(new MemoryTierFactory());
        int hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                strata.insert(insert);
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot insert: " + insert, e);
            }
            assertEquals(insert, strata.find(insert));
            strata.copacetic();
        }
        hashCode = 1;
        for (int i = 0; i < 99; i++)
        {
            hashCode = 37 * hashCode + i;
            Object insert = new Integer(hashCode);
            try
            {
                assertEquals(insert, strata.find(insert));
            }
            catch (Throwable e)
            {
                throw new RuntimeException("Cannot find: " + insert, e);
            }
            strata.copacetic();
        }
    }
    
    public void testInsertDuplicates()
    {
        
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */