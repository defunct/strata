package com.goodworkalan.strata;

import java.util.Iterator;
import java.util.LinkedList;


final class Mutation<B, A, X>
{
    private final X txn;

    private final Structure<B, A, X> structure;
    
    final B bucket;
    
    private final Comparable<B> _comparable;
    
    final Deletable<B> deletable;
    
    final LinkedList<Level<B, A, X>> listOfLevels = new LinkedList<Level<B, A, X>>();
    
    private boolean onlyChild;
    
    private boolean deleting;
    
    private B result;
    
    private B replacement;
    
    private LeafTier<B, A> leftLeaf;
    
    public LeafOperation<B, A, X> leafOperation;

    public Mutation(X txn,
                    Structure<B, A, X> structure,
                    B bucket,
                    Comparable<B> comparable,
                    Deletable<B> deletable)
    {
        this.txn = txn;
        this._comparable = comparable;
        this.deletable = deletable;
        this.bucket = bucket;
        this.structure = structure;
    }
    
    public Comparable<B> getComparable()
    {
        return _comparable;
    }
    
    public X getTxn()
    {
        return txn;
    }
    
    public B getBucket()
    {
        return bucket;
    }

    public Structure<B, A, X> getStructure()
    {
        return structure;
    }

    public B getResult()
    {
        return result;
    }
    
    public void setResult(B result)
    {
        this.result = result;
    }
    
    public B getReplacement()
    {
        return replacement;
    }
    
    public void setReplacement(B replacement)
    {
        this.replacement = replacement;
    }
    
    public LeafTier<B, A> getLeftLeaf()
    {
        return leftLeaf;
    }
    
    public void setLeftLeaf(LeafTier<B, A> leftLeaf)
    {
        this.leftLeaf = leftLeaf;
    }
    
    public boolean isOnlyChild()
    {
        return onlyChild;
    }
    
    public void setOnlyChild(boolean onlyChild)
    {
        this.onlyChild = onlyChild;
    }
    
    public boolean isDeleting()
    {
        return deleting;
    }
    
    public void setDeleting(boolean deleting)
    {
        this.deleting = deleting;
    }
    
    public InnerTier<B, A> newInnerTier(ChildType childType)
    {
        InnerTier<B, A> inner = new InnerTier<B, A>();
        inner.setAddress(getStructure().getAllocator().allocate(getTxn(), inner, getStructure().getInnerSize()));
        inner.setChildType(childType);
        return inner;
    }
    
    public LeafTier<B, A> newLeafTier()
    {
        LeafTier<B, A> leaf = new LeafTier<B, A>();
        leaf.setAddress(getStructure().getAllocator().allocate(getTxn(), leaf, getStructure().getInnerSize()));
        return leaf;
    }
    
    public void rewind(int leaveExclusive)
    {
        Iterator<Level<B, A, X>>levels = listOfLevels.iterator();
        int size = listOfLevels.size();
        boolean unlock = true;

        for (int i = 0; i < size - leaveExclusive; i++)
        {
            Level<B, A, X> level = levels.next();
            Iterator<Operation<B, A, X>> operations = level.listOfOperations.iterator();
            while (operations.hasNext())
            {
                Operation<B, A, X> operation = operations.next();
                if (operation.canCancel())
                {
                    operations.remove();
                }
                else
                {
                    unlock = false;
                }
            }
            if (unlock)
            {
                if (listOfLevels.size() == 3)
                {
                    level.downgrade();
                }
                else
                {
                    level.releaseAndClear();
                    levels.remove();
                }
            }
        }
    }

    public void shift()
    {
        Iterator<Level<B, A, X>> levels = listOfLevels.iterator();
        while (listOfLevels.size() > 3 && levels.hasNext())
        {
            Level<B, A, X> level = levels.next();
            if (level.listOfOperations.size() != 0)
            {
                break;
            }

            level.releaseAndClear();
            levels.remove();
        }
    }
    
    public Comparable<B> newComparable(B object)
    {
        return null;
    }
    
    public void clear()
    {
        
    }
}