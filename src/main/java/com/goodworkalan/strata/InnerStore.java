package com.goodworkalan.strata;

public interface InnerStore<T, A, X>
{
    public A allocate(X txn, int size);
    
    public <B> InnerTier<B, A> load(X txn, A address, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
    
    public <B> void write(X txn, InnerTier<B, A> inner, Cooper<T, B, X> cooper, Extractor<T, X> extractor);
    
    public void free(X txn, A address);
}