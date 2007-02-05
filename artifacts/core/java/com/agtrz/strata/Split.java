package com.agtrz.strata;

public class Split
{
    private final Object pivot;
    
    private final Tier right;

    public Split(Object pivot, Tier right)
    {
        this.pivot = pivot;
        this.right = right;
    }

    public Object getPivot()
    {
        return pivot;
    }

    public Tier getRight()
    {
        return right;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */