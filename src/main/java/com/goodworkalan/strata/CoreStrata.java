package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

/**
 * The core implementation of the b+tree. The b+tree is is split into
 * interface/implementation since the b+tree itself does not need to expose the
 * address type of the persistent storage strategy.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class CoreStrata<T, A>
implements Strata<T> {
    /** The address of the root inner tier. */
    private final A rootAddress;

    /** The collection of the core services of the b+tree. */
    private final Structure<T, A> structure;

    /**
     * Create a new b+tree implementation with the given root address and
     * bouquet of services.
     * 
     * @param rootAddress
     *            The root address of the b+tree root inner tier.
     * @param structure
     *            A bouquet of services.
     */
    public CoreStrata(A rootAddress, Structure<T, A> structure) {
        this.rootAddress = rootAddress;
        this.structure = structure;
    }

    /**
     * Get the address of the root inner tier.
     * 
     * @return The address of the root inner tier.
     */
    public A getRootAddress() {
        return rootAddress;
    }

    /**
     * Get the address of the root inner tier.
     * 
     * @return The address of the root inner tier.
     */
    public Schema<T> getSchema() {
        return structure.newSchema();
    }

    /**
     * Create a query of the b+tree.
     * 
     * @return A new query of the b+tree.
     */
    public Query<T> query() {
        return query(new Stash());
    }

    /**
     * Create a query of the b+tree using the given stash to communicate any
     * additional participants in the storage strategy.
     * 
     * @param stash
     *            A type-safe container of out of band data.
     * @return A new query of the b+tree.
     */
    public Query<T> query(Stash stash) {
        return new CoreQuery<T, A>(stash, this, structure);
    }
}