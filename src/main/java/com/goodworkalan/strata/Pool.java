package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for caching tiers in memory when as they are read from disk. The
 * tier pool is established by the storage strategy, so that the tier pool
 * implementation will load a page from the allocator if it is not already in
 * memory.
 * <p>
 * In memory tier pools will interpret the address to be an object reference and
 * will simply cast the address to an inner or leaf tier.
 * 
 * @author Alan Gutierrez
 * 
 * @param <Record>
 *            The value type of the indexed objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Pool<Record, Address> {
    /**
     * Get the tier for the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the tier cassette.
     * @return The tier.
     */
    public Tier<Record, Address> get(Stash stash, Address address);
}