package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface Storage<T, F extends Comparable<? super F>, A>
{
    // TODO Document.
    public InnerStore<T, F, A> getInnerStore();
    
    // TODO Document.
    public LeafStore<T, F, A> getLeafStore();
    
    // TODO Document.
    public void commit(Stash stash);
    
    // TODO Document.
    public A getNull();
    
    // TODO Document.
    public boolean isNull(A address);
}