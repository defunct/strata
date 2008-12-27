package com.goodworkalan.strata;

public interface Query<T>
{
    public void add(T object);
    
    public Cursor<T> find(Comparable<?>... fields);
    
    public Object remove(Deletable<T> deletable, Comparable<?>... fields);
    
    public Object remove(Comparable<?>... fields);

    public  Deletable<T> deleteAny();

}