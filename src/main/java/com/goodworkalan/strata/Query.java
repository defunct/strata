package com.goodworkalan.strata;

public interface Query<T, F extends Comparable<F>>
{
    public void add(T object);
    
    public Cursor<T> find(F fields);
    
    public T remove(Deletable<T> deletable, F fields);
    
    public T remove(F fields);

    public Deletable<T> deleteAny();
    
    public Cursor<T> first();
    
    public F extract(T object);
    
    public void flush();
}