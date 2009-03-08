package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public class ExtractorComparableFactory<T, F extends Comparable<? super F>>
implements ComparableFactory<T>
{
    // TODO Document.
    private final Extractor<T, F> extractor;
    
    // TODO Document.
    public ExtractorComparableFactory(Extractor<T, F> extractor)
    {
        this.extractor = extractor;
    }
    
    // TODO Document.
    public Comparable<T> newComparable(final Stash stash, T object)
    {
        final F field = extractor.extract(stash, object);
        return new Comparable<T>()
        {
            public int compareTo(T other)
            {
                return field.compareTo(extractor.extract(stash, other));
            }
        };
    }
}
