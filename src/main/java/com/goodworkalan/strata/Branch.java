package com.goodworkalan.strata;

/**
 * A mapping of a pivot value to a tier address where values greater than or
 * equal to the pivot value are stored.
 * 
 * @author Alan Gutierrez
 *
 * @param <B>
 *            The bucket type used to store index fields.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class Branch<B, A>
{
    /** The child tier address. */
    private final A address;

    /** The bucket containing index fields of this branch. */
    private B pivot;

    /**
     * Create a branch that maps the pivot value in the given bucket to the
     * given child tier address.
     * 
     * @param pivot The bucket containing index fields of this branch.
     * @param address The child tier address.
     */
    public Branch(B pivot, A address)
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
     * Get the bucket containing index fields of this branch.
     * 
     * @return The bucket.
     */
    public B getPivot()
    {
        return pivot;
    }

    /**
     * Set the bucket containing index fields of this branch.
     * 
     * @param pivot
     *            The bucket.
     */
    public void setPivot(B pivot)
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