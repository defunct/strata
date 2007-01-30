package com.agtrz.strata;

public class Branch
{
    private final Tier left;

    private final Object object;
    
    private final int count;

    public final static Object TERMINAL = new Object()
    {
        public String toString()
        {
            return "TERMINAL";
        }
    };

    public Branch(Tier left, Object object)
    {
        this.left = left;
        this.object = object;
        this.count = left.size();
    }

    public Tier getLeft()
    {
        return left;
    }

    public Object getObject()
    {
        return object;
    }
    
    public int getCount()
    {
        return count;
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