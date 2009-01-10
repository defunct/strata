package com.goodworkalan.strata;


public final class LeafTier<B, A>
extends Tier<B, A>
{
    private static final long serialVersionUID = 1L;

    private A next;
    
    public int find(Comparable<B> comparable)
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
    
    public <X> void link(Mutation<B, A> mutation, LeafTier<B, A> nextLeaf)
    {
        Structure<B, A> structure = mutation.getStructure();
        TierWriter<B, A> writer = structure.getWriter();
        writer.dirty(mutation.getTxn(), this);
        writer.dirty(mutation.getTxn(), nextLeaf);
        nextLeaf.setNext(getNext());
        setNext(nextLeaf.getAddress());
    }
    
    public <X> LeafTier<B, A> getNextAndLock(Mutation<B, A> mutation, Level<B, A> leafLevel)
    {
        Structure<B, A> structure = mutation.getStructure();
        if (!structure.getAllocator().isNull(getNext()))
        {
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), getNext());
            leafLevel.lockAndAdd(leaf);
            return leaf;
        }
        return null;
    }
    
    public <X> void append(Mutation<B, A> mutation, Level<B, A> leafLevel)
    {
        Structure<B, A> structure = mutation.getStructure();
        if (size() == structure.getLeafSize())
        {
            LeafTier<B, A> nextLeaf = getNextAndLock(mutation, leafLevel);
            if (null == nextLeaf || structure.compare(mutation.getTxn(), mutation.getBucket(), nextLeaf.get(0)) != 0)
            {
                nextLeaf = mutation.newLeafTier();
                link(mutation, nextLeaf);
            }
            nextLeaf.append(mutation, leafLevel);
        }
        else
        {
            add(mutation.getBucket());
            structure.getWriter().dirty(mutation.getTxn(), this);
        }
    }

    public <X> LeafTier<B, A> getNext(Mutation<B, A> mutation)
    {
        return mutation.getStructure().getPool().getLeafTier(mutation.getTxn(), getNext());
    }
    
    public A getNext()
    {
        return next;
    }
    
    public void setNext(A next)
    {
        this.next = next;
    }
}