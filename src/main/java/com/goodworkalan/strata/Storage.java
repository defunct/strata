package com.goodworkalan.strata;

/**
 * A strategy for persistent storage for inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public interface Storage<T, A>
{
    /**
     * Get the storage strategy for inner tiers.
     * 
     * @return The storage strategy for inner tiers.
     */
    public InnerStore<T, A> getInnerStore();

    /**
     * Get the storage strategy for leaf tiers.
     * 
     * @return The storage strategy for leaf tiers.
     */
    public LeafStore<T, A> getLeafStore();
    
    /**
     * Get the null address value for this allocation strategy.
     * 
     * @return The null address value.
     */
    public A getNull();

    /**
     * Return true if the given address is the null value for this allocation
     * strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(A address);
}