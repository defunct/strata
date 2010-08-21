package com.goodworkalan.strata.memory;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.Storage;
import com.goodworkalan.strata.Tier;

/**
 * A null persistent storage strategy for an in memory implementation of the
 * b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 */
final class InMemoryStorage<T> implements Storage<T, InMemoryTier<T>> {
    public Tier<T, InMemoryTier<T>> allocate(boolean leaf, int capacity) {
        return new InMemoryTier<T>();
    }
    
    public void free(Stash stash, InMemoryTier<T> address) {
    }
    
    public Tier<T, InMemoryTier<T>> load(Stash stash, InMemoryTier<T> address) {
        return address;
    }
    
    public void write(Stash stash, Tier<T, InMemoryTier<T>> tier) {
    }

    /**
     * Return a null super type token reference as the null address value for
     * this allocation strategy.
     * 
     * @return The null address value.
     */
    public InMemoryTier<T> getNull() {
        return null;
    }

    /**
     * Return true if the given address is null indicating that it is the null
     * value for this allocation strategy.
     * 
     * @param address
     *            A storage address.
     * @return True if the address is null.
     */
    public boolean isNull(InMemoryTier<T> address) {
        return address == null;
    }
}