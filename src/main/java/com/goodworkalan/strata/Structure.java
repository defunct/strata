package com.goodworkalan.strata;

/**
 * A bouquet of services.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class Structure<T, A>
{
    /** The capacity of branches of an inner tier. */
    private final int innerCapacity;
    
    /** The capacity of object values of a leaf tier. */
    private final int leafCapacity;

    /**
     * The allocator to use to allocate persistent storage of inner and leaf
     * tiers.
     */
    private final Allocator<T, A> allocator;
    
    /** A pool of tiers currently in memory. */
    private final TierPool<T, A> tierPool;

    /** The writer used to stage dirty pages for writing. */
    private final TierWriter<T, A> tierWriter;
    
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
     * @param allocator
     *            The allocator to use to allocate persistent storage of inner
     *            and leaf tiers.
     * @param tierPool
     *            A pool of tiers currently in memory.
     * @param tierWriter
     *            The writer used to stage dirty pages for writing.
     * @param comparableFactory
     *            The factory to use to create comparables for objects in the
     *            b+tree to compare against other object in the b+tree.
     */
    public Structure(int innerCapacity, int leafCapacity, Allocator<T, A> allocator, TierPool<T, A> tierPool, TierWriter<T, A> tierWriter, ComparableFactory<T> comparableFactory)
    {
        this.innerCapacity = innerCapacity;
        this.leafCapacity = leafCapacity;
        this.allocator = allocator;
        this.tierPool = tierPool;
        this.tierWriter = tierWriter;
        this.comparableFactory = comparableFactory;
    }

    /**
     * Get the capacity of branches of an inner tier.
     * 
     * @return The capacity of branches of an inner tier.
     */
    public int getInnerSize()
    {
        return innerCapacity;
    }
    

    /**
     * Get the capacity of object values of a leaf tier.
     * 
     * @return The capacity of object values of a leaf tier.
     */
    public int getLeafSize()
    {
        return leafCapacity;
    }

    /**
     * Get the allocator to use to allocate persistent storage of inner and leaf
     * tiers.
     * 
     * @return The allocator to use to allocate persistent storage of inner and
     *         leaf tiers.
     */
    public Allocator<T, A> getAllocator()
    {
        return allocator;
    }

    /**
     * Get the pool of tiers currently in memory.
     * 
     * @return The pool of tiers currently in memory.
     */
    public TierPool<T, A> getPool()
    {
        return tierPool;
    }

    /**
     * Get the writer used to stage dirty pages for writing.
     * 
     * @return The writer used to stage dirty pages for writing.
     */
    public TierWriter<T, A> getTierWriter()
    {
        return tierWriter;
    }

    /**
     * Get the factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     * 
     * @return The factory to use to create comparables for objects in the
     *         b+tree to compare against other object in the b+tree.
     */
    public ComparableFactory<T> getComparableFactory()
    {
        return comparableFactory;
    }
    
    /**
     * Create a schema using the properties of this bouquet of services.
     * 
     * @return A new schema.
     */
    public Schema<T> newSchema()
    {
        Schema<T> schema = new Schema<T>();
        schema.setInnerCapacity(getInnerSize());
        schema.setLeafCapacity(getLeafSize());
        schema.setComparableFactory(getComparableFactory());
        return schema;
    }
}