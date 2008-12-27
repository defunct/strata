package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class Tier<B, A>
extends ArrayList<B>
{
    private static final long serialVersionUID = 1L;

    private final ReadWriteLock readWriteLock;

    private A address;
    
    public Tier()
    {
        this.readWriteLock = new ReentrantReadWriteLock();
    }
    
    public ReadWriteLock getReadWriteLock()
    {
        return readWriteLock;
    }

    public A getAddress()
    {
        return address;
    }
    
    public void setAddress(A address)
    {
        this.address = address;
    }
    
    @Override
    public boolean equals(Object o)
    {
        return o == this;
    }
    
    @Override
    public int hashCode()
    {
        return System.identityHashCode(this);
    }
}