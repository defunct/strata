package com.goodworkalan.strata;

/**
 * The collection of the core services of the b+tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class Structure<T, A> {
    /** The capacity of branches of an inner tier. */
    private final int innerCapacity;

    /** The capacity of object values of a leaf tier. */
    private final int leafCapacity;

    /** The persistent storage strategy. */
    private final Storage<T, A> storage;

    /** The writer used to stage dirty pages for writing. */
    private final Stage<T, A> stage;
    
    /**
     * The factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     */
    private final ComparableFactory<T> comparableFactory;

    /**
     * Create a new structure.
     * 
     * @param innerCapacity
     *            The capacity of branches of an inner tier.
     * @param leafCapacity
     *            The capacity of object values of a leaf tier.
     * @param storage
     *            The persistent storage strategy.
     * @param tierPool
     *            A pool of tiers currently in memory.
     * @param tierWriter
     *            The writer used to stage dirty pages for writing.
     * @param comparableFactory
     *            The factory to use to create comparables for objects in the
     *            b+tree to compare against other object in the b+tree.
     */
    public Structure(int innerCapacity, int leafCapacity, Storage<T, A> storage, Stage<T, A> tierWriter, ComparableFactory<T> comparableFactory) {
        this.innerCapacity = innerCapacity;
        this.leafCapacity = leafCapacity;
        this.storage = storage;
        this.stage = tierWriter;
        this.comparableFactory = comparableFactory;
    }

    /**
     * Get the capacity of branches of an inner tier.
     * 
     * @return The capacity of branches of an inner tier.
     */
    public int getInnerSize() {
        return innerCapacity;
    }

    /**
     * Get the capacity of object values of a leaf tier.
     * 
     * @return The capacity of object values of a leaf tier.
     */
    public int getLeafSize() {
        return leafCapacity;
    }

    /**
     * Get the persistent storage strategy.
     * 
     * @return The persistent storage strategy.
     */
    public Storage<T, A> getStorage() {
        return storage;
    }

    /**
     * Get the writer used to stage dirty pages for writing.
     * 
     * @return The writer used to stage dirty pages for writing.
     */
    public Stage<T, A> getStage() {
        return stage;
    }

    /**
     * Get the factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     * 
     * @return The factory to use to create comparables for objects in the
     *         b+tree to compare against other object in the b+tree.
     */
    public ComparableFactory<T> getComparableFactory() {
        return comparableFactory;
    }

    /**
     * Create a schema using the properties of this bouquet of services.
     * 
     * @return A new schema.
     */
    public Schema<T> newSchema() {
        Schema<T> schema = new Schema<T>();
        schema.setInnerCapacity(getInnerSize());
        schema.setLeafCapacity(getLeafSize());
        schema.setComparableFactory(getComparableFactory());
        return schema;
    }
}