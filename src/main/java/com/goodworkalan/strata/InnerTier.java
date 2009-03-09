package com.goodworkalan.strata;

import java.util.Iterator;

/**
 * An inner level of the b-tree that references either inner levels or leaf
 * levels.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
class InnerTier<T, A>
extends Tier<Branch<T, A>, A>
{
    /** The serial version id. */
    private static final long serialVersionUID = 1L;

    /** The flag indicating whether child tiers are inner or leaf tiers. */
    private ChildType childType;

    /**
     * Get the flag indicating whether child tiers are inner or leaf tiers.
     * 
     * @return The flag indicating whether child tiers are inner or leaf tiers.
     */
    public ChildType getChildType()
    {
        return childType;
    }

    /**
     * Get the index of the branch with the given child tier address.
     * 
     * @param address
     *            The child tier address.
     * @return The index of the branch with the given child tier address or
     *         <code>-1</code> if not found.
     */
    public int getIndex(A address)
    {
        int index = 0;
        Iterator<Branch<T, A>> branches = iterator();
        while (branches.hasNext())
        {
            Branch<T, A> branch = branches.next();
            if (branch.getAddress().equals(address))
            {
                return index;
            }
            index++;
        }
        return -1;
    }

    /**
     * Set the type of child tier to one of inner or child.
     * 
     * @param childType
     *            The child tier type.
     */
    public void setChildType(ChildType childType)
    {
        this.childType = childType;
    }

    /**
     * Find the branch whose child tier is the path to objects that are equal to
     * or greater than the comparable.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return The branch whose child tier is the path to objects that are equal
     *         to or greater than the comparable.
     */
    public Branch<T, A> find(Comparable<? super T> comparable)
    {
        int low = 1;
        int high = size() - 1;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            int compare = comparable.compareTo(get(mid).getPivot());
            if (compare > 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }
        if (low < size())
        {
            Branch<T, A> branch = get(low);
            if (comparable.compareTo(branch.getPivot()) == 0)
            {
                return branch;
            }
        }
        return get(low - 1);
    }
}