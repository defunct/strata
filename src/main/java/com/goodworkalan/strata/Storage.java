package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

public interface Storage<T, F extends Comparable<F>, A>
{
    public InnerStore<T, F, A> getInnerStore();
    
    public LeafStore<T, F, A> getLeafStore();
    
    public void commit(Stash stash);
    
    public A getNull();
    
    public boolean isNull(A address);
}