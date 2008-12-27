package com.goodworkalan.strata;

public interface LeafStore<T, A, X>
{
    public A allocate(X txn, int size);
    
    public <B> LeafTier<B, A> load(X txn, A address, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
    
    public <B> void write(X txn, LeafTier<B, A> leaf, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
    
    public void free(X txn, A address);
}