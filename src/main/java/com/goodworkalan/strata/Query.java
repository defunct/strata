package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public interface Query<T, F extends Comparable<? super F>>
{
    public Stash getStash();
    
    public Strata<T, F> getStrata();
    
    public void add(T object);
    
    // TODO Rename find fields? (Erasure)
    public Cursor<T> find(Comparable<? super F> comparable);
    
    public T remove(Deletable<T> deletable, Comparable<? super F> comparable);
    
    public T remove(Comparable<? super F> comparable);

    public Deletable<T> deleteAny();
    
    public Cursor<T> first();
    
    public F extract(T object);
    
    public void flush();
    
    public void destroy();
}