package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

public interface LeafStore<T, F extends Comparable<F>, A>
{
    public A allocate(Stash stash, int size);
    
    public <B> LeafTier<B, A> load(Stash stash, A address, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    public <B> void write(Stash stash, LeafTier<B, A> leaf, Cooper<T, F, B> cooper, Extractor<T, F> extractor);
    
    public void free(Stash stash, A address);
}