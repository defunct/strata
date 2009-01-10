package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public interface InnerStore<T, F extends Comparable<F>, A>
{
    public A allocate(Stash stash, int size);
    
    public <B> InnerTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    public <B> void write(Stash stash, InnerTier<B, A> inner, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    public void free(Stash stash, A address);
}