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
    public <A> A create(Stash stash, Storage<T, A> storage)
    {
        Allocator<T, A> allocator = storage.getAllocator();
        
        InnerTier<T, A> root = new InnerTier<T, A>();
        root.setChildType(ChildType.LEAF);
        root.setAddress(allocator.allocate(stash, root, innerCapacity));
        
        LeafTier<T, A> leaf = new LeafTier<T, A>();
        leaf.setAddress(allocator.allocate(stash, leaf, leafCapacity));
        
        root.add(new Branch<T, A>(null, leaf.getAddress()));
 
        allocator.dirty(stash, root);
        allocator.dirty(stash, leaf);
        
        return root.getAddress();
    }
    
    // TODO Document.
    public <A> Strata<T> inMemory(Stash stash, Ilk<T> ilk)
    {
        InMemoryStorage<T> inMemoryStorage = new InMemoryStorage<T>(ilk);
        Ilk.Pair root = create(stash, inMemoryStorage);
        return open(stash, root, inMemoryStorage);
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
     * @param storage
     *            The persistent storage strategy.
     * @return The opened b+tree.
     */
    public <A> Strata<T> open(Stash stash, A rootAddress, Storage<T, A> storage)
    {
        Allocator<T, A> allocator = storage.getAllocator();
        TierPool<T, A> pool = storage.getTierPool();
        Structure<T, A> structure = new Structure<T, A>(innerCapacity, leafCapacity, allocator, pool, comparableFactory);
        return new CoreStrata<T, A>(rootAddress, structure);
    }
}