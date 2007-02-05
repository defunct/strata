package com.agtrz.strata;

public class Branch
{
    private final Object keyOfLeft;
    
    private final Object keyOfObject;

    private final Object object;

    private int size;

    public final static Object TERMINAL = new Object()
    {
        public String toString()
        {
            return "TERMINAL";
        }
    };

    public Branch(Object keyOfLeft, Object keyOfObject, Object object, int size)
    {
        this.keyOfLeft = keyOfLeft;
        this.object = object;
        this.keyOfObject = keyOfObject;
        this.size = size;
    }

    public Object getKeyOfLeft()
    {
        return keyOfLeft;
    }

    public Object getKeyOfObject()
    {
        return keyOfObject;
    }
    
    // FIXME rename pivot.
    
    public Object getObject()
    {
        return object;
    }

    public int getSize()
    {
        return size;
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public boolean isTerminal()
    {
        return TERMINAL == object;
    }

    public String toString()
    {
        return object.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */