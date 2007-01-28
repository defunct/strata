package com.agtrz.strata;

public class Split
{
    private final Object key;

    private final Tier right;

    public Split(Object key, Tier right)
    {
        this.key = key;
        this.right = right;
    }

    public Object getKey()
    {
        return key;
    }

    public Tier getRight()
    {
        return right;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */