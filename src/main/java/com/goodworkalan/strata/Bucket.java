package com.goodworkalan.strata;

public final class Bucket<T>
{
    private final Comparable<?>[] fields;

    private final T object;

    public Bucket(Comparable<?>[] fields, T object)
    {
        this.fields = fields;
        this.object = object;
    }
    
    public Comparable<?>[] getFields()
    {
        return fields;
    }
    
    public T getObject()
    {
        return object;
    }
}