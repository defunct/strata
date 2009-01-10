package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

public interface Extractor<T, F extends Comparable<F>>
{
    public F extract(Stash stash, T object);
}