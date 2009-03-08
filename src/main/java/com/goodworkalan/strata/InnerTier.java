package com.goodworkalan.strata;

import java.util.Iterator;

// FIXME Document.
public class InnerTier<T, A>
extends Tier<Branch<T, A>, A>
{
    // TODO Document.
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
    
    // TODO Document.
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
    
    // TODO Document.
    public void setChildType(ChildType childType)
    {
        this.childType = childType;
    }

    // TODO Document.
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