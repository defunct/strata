package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public class CastComparableFactory<T extends Comparable<? super T>>
implements ComparableFactory<T>
{
    public Comparable<? super T> newComparable(Stash stash, T object)
    {
        return object;
    }
}
