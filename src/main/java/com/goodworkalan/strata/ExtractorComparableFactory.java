package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * Creates a comparable based on the given object that will compare the given
 * object to objects of the same type using a comparable value extracted from
 * both objects by the extractor property.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <F>
 *            A comparable type derived from the b+tree value type.
 */
public class ExtractorComparableFactory<T, F extends Comparable<? super F>>
implements ComparableFactory<T>
{
    /** The extractor to use to obtain a comparable value. */
    private final Extractor<T, F> extractor;

    /**
     * Create a new extractor comparable factory that uses the given extractor
     * to obtain a comparable value from a b+tree object value.
     * 
     * @param extractor
     *            The extractor.
     */
    public ExtractorComparableFactory(Extractor<T, F> extractor)
    {
        this.extractor = extractor;
    }

    /**
     * Create a comparable based on the given object that will compare the given
     * object to objects of the same type using a comparable value extracted
     * from both objects by the extractor property.
     * 
     * @return An extracted comparable comparator.
     */
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
