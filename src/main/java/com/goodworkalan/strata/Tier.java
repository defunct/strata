package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A level in the b+tree and the base class for inner and leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public abstract class Tier<T, A>
extends ArrayList<T>
{
    /** The serial version id. */
    private static final long serialVersionUID = 1L;

    /** The read/write lock used to guard this tier. */
    private final ReadWriteLock readWriteLock;

    /** The address of the tier in persistent storage. */
    private A address;
    
    /**
     * Create a new tier.
     */
    public Tier()
    {
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    /**
     * Get the read/write lock used to guard this tier.
     * 
     * @return The read/write lock used to guard this tier.
     */
    public ReadWriteLock getReadWriteLock()
    {
        return readWriteLock;
    }

    /**
     * Get the address of the tier in persistent storage.
     * 
     * @return The address of the tier in persistent storage.
     */
    public A getAddress()
    {
        return address;
    }

    /**
     * Set the address of the tier in persistent storage.
     * 
     * @param address
     *            The address of the tier in persistent storage.
     */
    public void setAddress(A address)
    {
        this.address = address;
    }

    /**
     * The equals method is overridden from array list to implement identity
     * equality since tiers are not copied or duplicated. This allows tiers to
     * be used in sets and as hash keys.
     * 
     * @return True if the given object is this object.
     */
    @Override
    public boolean equals(Object object)
    {
        return object == this;
    }

    /**
     * The hash code method is overridden from array list to implement identity
     * equality since tiers are not copied or duplicated. This allows tiers to
     * be used in sets and as hash keys.
     * 
     * @return True if the given object is this object.
     */
    @Override
    public int hashCode()
    {
        return System.identityHashCode(this);
    }
}