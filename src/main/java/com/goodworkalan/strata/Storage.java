package com.goodworkalan.strata;

public interface Storage<T, A, X>
{
    public InnerStore<T, A, X> getInnerStore();
    
    public LeafStore<T, A, X> getLeafStore();
    
    public void commit(X txn);
    
    public A getNull();
    
    public boolean isNull(A address);
}