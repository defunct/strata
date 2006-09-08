package com.agtrz.strata;

import junit.framework.TestCase;

import com.agtrz.util.ObjectReadBuffer;
import com.agtrz.util.ObjectWriteBuffer;

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
        strata.insert(ALPHABET[0]);
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */