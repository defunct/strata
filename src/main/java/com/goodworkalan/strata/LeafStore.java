package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public interface LeafStore<T, F extends Comparable<? super F>, A>
{
    // TODO Document.
    public A allocate(Stash stash, int size);
    
    // TODO Document.
    public <B> LeafTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    // TODO Document.
    public <B> void write(Stash stash, LeafTier<B, A> leaf, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    // TODO Document.
    public void free(Stash stash, A address);
}