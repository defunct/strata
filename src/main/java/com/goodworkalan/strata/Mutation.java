package com.goodworkalan.strata;

import java.util.Iterator;
import java.util.LinkedList;

import com.goodworkalan.favorites.Stash;


final class Mutation<B, A>
{
    private final Stash stash;

    private final Structure<B, A> structure;
    
    final B bucket;
    
    private final Comparable<B> _comparable;
    
    final Deletable<B> deletable;
    
    final LinkedList<Level<B, A>> listOfLevels = new LinkedList<Level<B, A>>();
    
    private boolean onlyChild;
    
    private boolean deleting;
    
    private B result;
    
    private B replacement;
    
    private LeafTier<B, A> leftLeaf;
    
    public LeafOperation<B, A> leafOperation;

    public Mutation(Stash stash,
                    Structure<B, A> structure,
                    B bucket,
                    Comparable<B> comparable,
                    Deletable<B> deletable)
    {
        this.stash = stash;
        this._comparable = comparable;
        this.deletable = deletable;
        this.bucket = bucket;
        this.structure = structure;
    }
    
    public Comparable<B> getComparable()
    {
        return _comparable;
    }
    
    // FIXME Rename.
    public Stash getTxn()
    {
        return stash;
    }
    
    public B getBucket()
    {
        return bucket;
    }

    public Structure<B, A> getStructure()
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
        Iterator<Level<B, A>>levels = listOfLevels.iterator();
        int size = listOfLevels.size();
        boolean unlock = true;

        for (int i = 0; i < size - leaveExclusive; i++)
        {
            Level<B, A> level = levels.next();
            Iterator<Operation<B, A>> operations = level.listOfOperations.iterator();
            while (operations.hasNext())
            {
                Operation<B, A> operation = operations.next();
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
        Iterator<Level<B, A>> levels = listOfLevels.iterator();
        while (listOfLevels.size() > 3 && levels.hasNext())
        {
            Level<B, A> level = levels.next();
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