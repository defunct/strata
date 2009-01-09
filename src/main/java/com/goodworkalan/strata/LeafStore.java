package com.goodworkalan.strata;

public interface LeafStore<T, F extends Comparable<F>, A, X>
{
    public A allocate(X txn, int size);
    
    public <B> LeafTier<B, A> load(X txn, A address, Cooper<T, F, B, X> cooper, Extractor<T, F, X> extractor);
    
    public <B> void write(X txn, LeafTier<B, A> leaf, Cooper<T, F, B, X> cooper, Extractor<T, F, X> extractor);
    
    public void free(X txn, A address);
}