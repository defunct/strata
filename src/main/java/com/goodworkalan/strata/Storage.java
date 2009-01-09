package com.goodworkalan.strata;

public interface Storage<T, F extends Comparable<F>, A, X>
{
    public InnerStore<T, F, A, X> getInnerStore();
    
    public LeafStore<T, F, A, X> getLeafStore();
    
    public void commit(X txn);
    
    public A getNull();
    
    public boolean isNull(A address);
}