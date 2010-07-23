package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * A strategy for persistent storage for inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <Record>
 *            The value type of the b+tree objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Storage<Record, Address> {
    /**
     * Allocate persistent storage for the given inner tier that can hold the
     * given capacity of branches. The inner tier itself is given so that the in
     * memory strategy can return a reference to it as the address value.
     * 
     * @return A new tier cassette.
     */
    public Tier<Record, Address> allocate(boolean leaf, int capacity);
    
    /**
     * Load a collection of inner tier branches from the persistent storage at
     * the given address.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of the inner tier storage.
     * @param objects
     *            The collection to load.
     * @return The tier cassette.
     */
    public Tier<Record, Address> load(Stash stash, Address address);

    /**
     * Write a storage cassette.
     * 
     * @param stash
     *            The type-safe container of out of band data.
     * @param tier
     *            The tier.
     */
    public void write(Stash stash, Tier<Record, Address> tier);

    /**
     * Free the tier at the address.
     * 
     * @param stash
     *            The type-safe container of out of band data.
     * @param address
     *            The address of the tier.
     */
    public void free(Stash stash, Address address);
    
    /**
     * Get the null address value for this allocation strategy.
     * 
     * @return The null address value.
     */
    public Address getNull();

    /**
     * Return true if the given address is the null value for this allocation
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(Address address);
}