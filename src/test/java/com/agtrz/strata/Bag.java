/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Bag<O, E extends Comparable<? super E>>
{
    public interface Extractor<O, E extends Comparable<? super E>>
    {
        E extract(O o);
    }
   
    public void add(O o);
    
    public O get(O o);
    
    public boolean contains(O o);
    
    public Cursor<O> find(E e);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */