package com.goodworkalan.strata;

// TODO Document.
public final class LeafTier<T, A>
extends Tier<T, A>
{
    // TODO Document.
    private static final long serialVersionUID = 1L;

    // TODO Document.
    private A next;
    
    // TODO Document.
    public int find(Comparable<? super T> comparable)
    {
        int low = 1;
        int high = size() - 1;
        while (low < high)
        {
            int mid = (low + high) >>> 1;
            int compare = comparable.compareTo(get(mid));
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
            while (low != 0 && comparable.compareTo(get(low - 1)) == 0)
            {
                low --;
            }
            return low;
        }
        return low - 1;
    }
    
    // TODO Document.
    public void link(Mutation<T, A> mutation, LeafTier<T, A> nextLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();
        Stage<T, A> writer = structure.getTierWriter();
        writer.dirty(mutation.getStash(), this);
        writer.dirty(mutation.getStash(), nextLeaf);
        nextLeaf.setNext(getNext());
        setNext(nextLeaf.getAddress());
    }
    
    // TODO Document.
    public LeafTier<T, A> getNextAndLock(Mutation<T, A> mutation, Level<T, A> leafLevel)
    {
        Structure<T, A> structure = mutation.getStructure();
        if (!structure.getAllocator().isNull(getNext()))
        {
            LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), getNext());
            leafLevel.lockAndAdd(leaf);
            return leaf;
        }
        return null;
    }
    
    // TODO Document.
    public void append(Mutation<T, A> mutation, Level<T, A> leafLevel)
    {
        Structure<T, A> structure = mutation.getStructure();
        if (size() == structure.getLeafSize())
        {
            LeafTier<T, A> nextLeaf = getNextAndLock(mutation, leafLevel);
            if (null == nextLeaf || structure.getComparableFactory().newComparable(mutation.getStash(), mutation.getObject()).compareTo(nextLeaf.get(0)) != 0)
            {
                nextLeaf = mutation.newLeafTier();
                link(mutation, nextLeaf);
            }
            nextLeaf.append(mutation, leafLevel);
        }
        else
        {
            add(mutation.getObject());
            structure.getTierWriter().dirty(mutation.getStash(), this);
        }
    }

    // TODO Document.
    public LeafTier<T, A> getNext(Mutation<T, A> mutation)
    {
        return mutation.getStructure().getPool().getLeafTier(mutation.getStash(), getNext());
    }
    
    // TODO Document.
    public A getNext()
    {
        return next;
    }
    
    // TODO Document.
    public void setNext(A next)
    {
        this.next = next;
    }
}