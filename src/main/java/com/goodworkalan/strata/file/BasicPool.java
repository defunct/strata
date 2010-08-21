package com.goodworkalan.strata.file;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.goodworkalan.stash.Stash;
import com.goodworkalan.strata.KeyedReference;
import com.goodworkalan.strata.Pool;
import com.goodworkalan.strata.Storage;
import com.goodworkalan.strata.Tier;
import com.goodworkalan.strata.Unmappable;

/**
 * A basic caching tier pool that maintains a map of softly referenced tiers
 * in memory.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class BasicPool<T, A> implements Pool<T, A> {
    /** A queue of references to inner tiers. */
    private final ReferenceQueue<Tier<T, A>> queue = new ReferenceQueue<Tier<T,A>>();

    /** A map of addresses to inner tiers. */
    private final ConcurrentMap<A, Reference<Tier<T, A>>> cassettes = new ConcurrentHashMap<A, Reference<Tier<T,A>>>();

    /** The allocator to use to load pages from disk. */
    private final Storage<T, A> storage;

    /**
     * Create a new basic tier pool.
     * 
     * @param storage
     *            The storage to use to load pages from disk.
     */
    public BasicPool(Storage<T, A> storage) {
        this.storage = storage;
    }

    /**
     * Remove unreferenced inner and leaf tiers from the address to tier maps.
     */
    private void collect() {
        Unmappable unmappable = null;
        while ((unmappable = (Unmappable) queue.poll()) != null) {
            unmappable.unmap();
        }
    }

    /**
     * Get the inner tier for the given address. The basic pool will keep a soft
     * reference to the inner tier in an in memory cache. If the inner tier is
     * available in the in memory cache, it is returned. If it is not available,
     * it is loaded and cached.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @param address
     *            The address of an inner tier.
     * @return The inner tier for the given address.
     */
    public Tier<T, A> get(Stash stash, A address) {
        collect();
        return get(stash, address, cassettes.get(address));
    }

    /**
     * Read the cassette at the address from file if it does not exist in the
     * concurrent maap.
     * <p>
     * Here's why this works: locks for read and write are elsewhere, so you are
     * reading because you have a valid read lock, and the only thing we really
     * need to synchronize is map access.
     * 
     * @param stash
     *            The type-safe container of out of band data.
     * @param address
     *            The address of a tier cassette.
     * @param reference
     *            The reference obtained from the concurrent map.
     * @return The referenced tier cassette or a tier cassette read from storage
     *         if the reference tier is null or collected.
     */
    public Tier<T, A> get(Stash stash, A address, Reference<Tier<T, A>> reference) {
        Tier<T, A> cassette = null;

        if (reference != null) {
            cassette = reference.get();
        }
        
        if (cassette == null) {
            cassette = storage.load(stash, address);
            return get(stash, address, cassettes.putIfAbsent(cassette.getAddress(), new KeyedReference<A, Tier<T,A>>(address, cassette, cassettes, queue)));
        }

        return cassette;
    }
}