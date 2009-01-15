package com.goodworkalan.strata;

public final class Bucket<T, F extends Comparable<? super F>>
{
    private final F fields;

    private final T object;

    public Bucket(F fields, T object)
    {
        this.fields = fields;
        this.object = object;
    }
    
    public F getFields()
    {
        return fields;
    }
    
    public T getObject()
    {
        return object;
    }
}