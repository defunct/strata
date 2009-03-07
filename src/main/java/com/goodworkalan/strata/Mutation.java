package com.goodworkalan.strata;

import java.util.Iterator;
import java.util.LinkedList;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class Mutation<B, A>
{
    // TODO Document.
    private final Stash stash;

    // TODO Document.
    private final Structure<B, A> structure;
    
    // TODO Document.
    final B bucket;
    
    // TODO Document.
    private final Comparable<B> comparable;
    
    // TODO Document.
    final Deletable<B> deletable;
    
    // TODO Document.
    final LinkedList<Level<B, A>> listOfLevels = new LinkedList<Level<B, A>>();
    
    // TODO Document.
    private boolean onlyChild;
    
    // TODO Document.
    private boolean deleting;
    
    // TODO Document.
    private B result;
    
    // TODO Document.
    private B replacement;
    
    // TODO Document.
    private LeafTier<B, A> leftLeaf;
    
    // TODO Document.
    public LeafOperation<B, A> leafOperation;

    // TODO Document.
    public Mutation(Stash stash,
                    Structure<B, A> structure,
                    B bucket,
                    Comparable<B> comparable,
                    Deletable<B> deletable)
    {
        this.stash = stash;
        this.comparable = comparable;
        this.deletable = deletable;
        this.bucket = bucket;
        this.structure = structure;
    }
    
    // TODO Document.
    public Comparable<B> getComparable()
    {
        return comparable;
    }
    
    // TODO Document.
    public Stash getStash()
    {
        return stash;
    }
    
    // TODO Document.
    public B getBucket()
    {
        return bucket;
    }

    // TODO Document.
    public Structure<B, A> getStructure()
    {
        return structure;
    }

    // TODO Document.
    public B getResult()
    {
        return result;
    }
    
    // TODO Document.
    public void setResult(B result)
    {
        this.result = result;
    }
    
    // TODO Document.
    public B getReplacement()
    {
        return replacement;
    }
    
    // TODO Document.
    public void setReplacement(B replacement)
    {
        this.replacement = replacement;
    }
    
    // TODO Document.
    public LeafTier<B, A> getLeftLeaf()
    {
        return leftLeaf;
    }
    
    // TODO Document.
    public void setLeftLeaf(LeafTier<B, A> leftLeaf)
    {
        this.leftLeaf = leftLeaf;
    }
    
    // TODO Document.
    public boolean isOnlyChild()
    {
        return onlyChild;
    }
    
    // TODO Document.
    public void setOnlyChild(boolean onlyChild)
    {
        this.onlyChild = onlyChild;
    }
    
    // TODO Document.
    public boolean isDeleting()
    {
        return deleting;
    }
    
    // TODO Document.
    public void setDeleting(boolean deleting)
    {
        this.deleting = deleting;
    }
    
    // TODO Document.
    public InnerTier<B, A> newInnerTier(ChildType childType)
    {
        InnerTier<B, A> inner = new InnerTier<B, A>();
        inner.setAddress(getStructure().getAllocator().allocate(getStash(), inner, getStructure().getInnerSize()));
        inner.setChildType(childType);
        return inner;
    }
    
    // TODO Document.
    public LeafTier<B, A> newLeafTier()
    {
        LeafTier<B, A> leaf = new LeafTier<B, A>();
        leaf.setAddress(getStructure().getAllocator().allocate(getStash(), leaf, getStructure().getInnerSize()));
        return leaf;
    }
    
    // TODO Document.
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

    // TODO Document.
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
    
    // TODO Document.
    public Comparable<B> newComparable(B object)
    {
        return null;
    }
    
    // TODO Document.
    public void clear()
    {
        
    }
}