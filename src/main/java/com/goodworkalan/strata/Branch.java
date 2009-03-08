package com.goodworkalan.strata;

/**
 * A mapping of a pivot value to a tier address where values greater than or
 * equal to the pivot value are stored.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the indexed objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class Branch<T, A>
{
    /** The child tier address. */
    private final A address;

    /** The branch value. */
    private T pivot;

    /**
     * Create a branch that maps the pivot value in the given bucket to the
     * given child tier address.
     * 
     * @param pivot
     *            The branch value.
     * @param address
     *            The child tier address.
     */
    public Branch(T pivot, A address)
    {
        this.address = address;
        this.pivot = pivot;
    }

    /**
     * Get the address of the child tier.
     * 
     * @return The child tier address.
     */
    public A getAddress()
    {
        return address;
    }

    /**
     * Get the branch value.
     * 
     * @return The branch value.
     */
    public T getPivot()
    {
        return pivot;
    }

    /**
     * Set the branch value.
     * 
     * @param pivot
     *            The branch value.
     */
    public void setPivot(T pivot)
    {
        this.pivot = pivot;
    }

    /**
     * Create a string representation of the branch.
     * 
     * @return A string representation.
     */
    public String toString()
    {
        return pivot == null ? "MINIMAL" : pivot.toString();
    }
}