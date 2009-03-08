package com.goodworkalan.strata;


/**
 * A strategy for persistent storage of inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Storage<T, A>
{
    /**
     * Get the strategy for allocating persistent storage for inner and leaf tiers.
     * @return The strategy for allocating persistent storage for inner and leaf tiers.
     */
    public Allocator<T, A> getAllocator();

    public TierPool<T, A> getTierPool();
}