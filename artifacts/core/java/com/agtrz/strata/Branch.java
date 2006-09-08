package com.agtrz.strata;

public class Branch
{
    private final Tier left;
    
    private final Object object;
    
    public final static Object TERMINAL = new Object();
    
    public Branch(Tier left, Object object)
    {
        this.left = left;
        this.object = object;
    }

    public Tier getLeft()
    {
        return left;
    }
    
    public Object getObject()
    {
        return object;
    }
    
    public boolean isTerminal()
    {
        return TERMINAL == object;
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */