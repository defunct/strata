package com.goodworkalan.strata;

import java.util.Iterator;

// TODO Document.
public class InnerTier<B, A>
extends Tier<Branch<B, A>, A>
{
    // TODO Document.
    private static final long serialVersionUID = 1L;

    // TODO Document.
    private ChildType childType;

    // TODO Document.
    public ChildType getChildType()
    {
        return childType;
    }
    
    // TODO Document.
    public int getIndex(A address)
    {
        int index = 0;
        Iterator<Branch<B, A>> branches = iterator();
        while (branches.hasNext())
        {
            Branch<B, A> branch = branches.next();
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
    public Branch<B, A> find(Comparable<B> comparable)
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
            Branch<B, A> branch = get(low);
            if (comparable.compareTo(branch.getPivot()) == 0)
            {
                return branch;
            }
        }
        return get(low - 1);
    }
}