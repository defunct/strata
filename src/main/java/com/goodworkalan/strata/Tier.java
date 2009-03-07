package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TODO Document.
public abstract class Tier<B, A>
extends ArrayList<B>
{
    // TODO Document.
    private static final long serialVersionUID = 1L;

    // TODO Document.
    private final ReadWriteLock readWriteLock;

    // TODO Document.
    private A address;
    
    // TODO Document.
    public Tier()
    {
        this.readWriteLock = new ReentrantReadWriteLock();
    }
    
    // TODO Document.
    public ReadWriteLock getReadWriteLock()
    {
        return readWriteLock;
    }

    // TODO Document.
    public A getAddress()
    {
        return address;
    }
    
    // TODO Document.
    public void setAddress(A address)
    {
        this.address = address;
    }
    
    // TODO Document.
    @Override
    public boolean equals(Object o)
    {
        return o == this;
    }
    
    // TODO Document.
    @Override
    public int hashCode()
    {
        return System.identityHashCode(this);
    }
}