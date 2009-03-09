package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

/**
 * A builder pattern for b+trees that defines the ordering and size of
 * leaves as properties of the builder, creating new b+trees from a given
 * persistent storage strategy.  
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 */
public final class Schema<T>
{
    /** The capacity of branches of an inner tier. */
    private int innerCapacity;
    
    /** The capacity of object values of a leaf tier. */
    private int leafCapacity;
    
    /** The number of tiers to keep in memory as dirty before writing. */
    private int maxDirtyTiers;
    
    /**
     * The factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     */
    private ComparableFactory<T> comparableFactory;
    
    /**
     * Set the capacity of branches of an inner tier.
     * 
     * @param capacity
     *            The capacity of branches of an inner tier.
     */
    public void setInnerCapacity(int capacity)
    {
        this.innerCapacity = capacity;
    }

    /**
     * Get the capacity of branches of an inner tier.
     * 
     * @return The capacity of branches of an inner tier.
     */
    public int getInnerCapacity()
    {
        return innerCapacity;
    }

    /**
     * Set the capacity of object values of a leaf tier.
     * 
     * @param capacity
     *            The capacity of object values of a leaf tier.
     */
    public void setLeafCapacity(int capacity)
    {
        this.leafCapacity = capacity;
    }

    /**
     * Set the capacity of object values of a leaf tier.
     * 
     * @return The capacity of object values of a leaf tier.
     */
    public int getLeafCapacity()
    {
        return leafCapacity;
    }

    /**
     * Set the factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     * 
     * @param comparableFactory
     *            The factory to use to create comparables.
     */
    public void setComparableFactory(ComparableFactory<T> comparableFactory)
    {
        this.comparableFactory = comparableFactory;
    }

    /**
     * Get the factory to use to create comparables for objects in the b+tree to
     * compare against other object in the b+tree.
     * 
     * @return The factory to use to create comparables.
     */
    public ComparableFactory<T> getComparableFactory()
    {
        return comparableFactory;
    }
    
    /**
     * Set the number of tiers to keep in memory as dirty before writing.
     * 
     * @param maxDirtyTiers
     *            The number of tiers to keep in memory as dirty before writing.
     */
    public void setMaxDirtyTiers(int maxDirtyTiers)
    {
        this.maxDirtyTiers = maxDirtyTiers;
    }

    /**
     * Get the number of tiers to keep in memory as dirty before writing.
     * 
     * @return The number of tiers to keep in memory as dirty before writing.
     */
    public int getMaxDirtyTiers()
    {
        return maxDirtyTiers;
    }
    
    /**
     * Create a new b+tree with the given storage strategy using the properties
     * of this schema.
     * 
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param stash
     *            A type-safe container of out of band data.
     * @param storage
     *            The persistent storage strategy.
     * @return A new b+tree.
     */
    public <A> A create(Stash stash, Storage<T, A> allocator)
    {
        InnerTier<T, A> root = new InnerTier<T, A>();
        root.setChildType(ChildType.LEAF);
        root.setAddress(allocator.allocate(stash, root, innerCapacity));
        
        LeafTier<T, A> leaf = new LeafTier<T, A>();
        leaf.setAddress(allocator.allocate(stash, leaf, leafCapacity));
        
        root.add(new Branch<T, A>(null, leaf.getAddress()));
 
        allocator.write(stash, root);
        allocator.write(stash, leaf);
        
        return root.getAddress();
    }

    /**
     * Create an in memory b+tree.
     * 
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param stash
     *            A type-safe container of out of band data.
     * @param ilk
     *            A super type token of the value type.
     * @return An in memory b+tree.
     */
    public <A> Strata<T> inMemory(Stash stash, Ilk<T> ilk)
    {
        NullStorage<T> allocator = new NullStorage<T>(ilk.key);
        ObjectReferencePool<T> pool = new ObjectReferencePool<T>(ilk.key);
        Ilk.Pair rootAddress = create(stash, allocator);
        Stage<T, Ilk.Pair> writer = new Stage<T, Ilk.Pair>(allocator, 0);
        return open(stash, rootAddress, pool, writer, allocator);
    }
    
    /**
     * Open an existing b+tree at the given root address with the given
     * persistent storage strategy.
     * 
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param stash
     *            A type-safe container of out of band data.
     * @param rootAddress
     *            The root address of the b+tree root inner tier.
     * @param allocator
     *            The persistent storage strategy.
     * @return The opened b+tree.
     */
    public <A> Strata<T> open(Stash stash, A rootAddress, Storage<T, A> allocator)
    {
        Pool<T, A> pool = new BasicPool<T, A>(allocator);
        Stage<T, A> writer = new Stage<T, A>(allocator, maxDirtyTiers);
        return open(stash, rootAddress, pool, writer, allocator);
    }

    /**
     * Open an existing b+tree at the given root address with the given
     * persistent storage strategy.
     * 
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param stash
     *            A type-safe container of out of band data.
     * @param rootAddress
     *            The root address of the b+tree root inner tier.
     * @param pool
     *            A pool of tiers currently in memory.
     * @param writer
     *            The writer used to stage dirty pages for writing.
     * @param allocator
     *            The persistent storage strategy.
     * @return The opened b+tree.
     */
    private <A> Strata<T> open(Stash stash, A rootAddress, Pool<T, A> pool, Stage<T, A> writer, Storage<T, A> allocator)
    {
        Structure<T, A> structure = new Structure<T, A>(innerCapacity, leafCapacity, allocator, pool, writer, comparableFactory);
        return new CoreStrata<T, A>(rootAddress, structure);
    }
}