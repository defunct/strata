package com.goodworkalan.strata;

// TODO Document.
public final class Bucket<T, F extends Comparable<? super F>>
{
    // TODO Document.
    private final F fields;

    // TODO Document.
    private final T object;

    // TODO Document.
    public Bucket(F fields, T object)
    {
        this.fields = fields;
        this.object = object;
    }
    
    // TODO Document.
    public F getFields()
    {
        return fields;
    }
    
    // TODO Document.
    public T getObject()
    {
        return object;
    }
}