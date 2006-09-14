package com.agtrz.strata;

public class Split
{
    private final Object key;
    
    private final Tier left;
    
    private final Tier right;
    
    public Split(Object key, Tier left, Tier right)
    {
        this.key = key;
        this.left = left;
        this.right = right;
    }

    public Object getKey()
    {
        return key;
    }

    public Tier getLeft()
    {
        return left;
    }

    public Tier getRight()
    {
        return right;
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */