package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface InnerStore<T, F extends Comparable<? super F>, A>
{
    // TODO Document.
    public A allocate(Stash stash, int size);
    
    // TODO Document.
    public <B> InnerTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    // TODO Document.
    public <B> void write(Stash stash, InnerTier<B, A> inner, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    // TODO Document.
    public void free(Stash stash, A address);
}