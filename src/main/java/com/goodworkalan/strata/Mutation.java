package com.goodworkalan.strata;

import java.util.Iterator;
import java.util.LinkedList;

import com.goodworkalan.stash.Stash;

// FIXME Document.
final class Mutation<T, A>
{
    /** The type-safe container of out of band data. */
    private final Stash stash;

    // TODO Document.
    private final Structure<T, A> structure;
    
    // TODO Document.
    private final Comparable<? super T> comparable;
    
    private final T object;
    
    // TODO Document.
    final Deletable<T> deletable;
    
    // TODO Document.
    final LinkedList<Level<T, A>> listOfLevels = new LinkedList<Level<T, A>>();
    
    // TODO Document.
    private boolean onlyChild;
    
    // TODO Document.
    private boolean deleting;
    
    /** The mutation result. */
    private T result;
    
    // TODO Document.
    private T replacement;
    
    // TODO Document.
    private LeafTier<T, A> leftLeaf;
    
    // TODO Document.
    public LeafOperation<T, A> leafOperation;

    // TODO Document.
    public Mutation(Stash stash, Structure<T, A> structure, Comparable<? super T> comparable, T object, Deletable<T> deletable)
    {
        this.stash = stash;
        this.comparable = comparable;
        this.deletable = deletable;
        this.object = object;
        this.structure = structure;
    }
    
    // TODO Document.
    public Comparable<? super T> getComparable()
    {
        return comparable;
    }
    
    // TODO Document.
    public Stash getStash()
    {
        return stash;
    }
    
    public T getObject()
    {
        return object;
    }
    
    // TODO Document.
    public Structure<T, A> getStructure()
    {
        return structure;
    }

    /**
     * Get the result of a delete operation, the object deleted from the tree if
     * any.
     * 
     * @return The mutation result.
     */
    public T getResult()
    {
        return result;
    }

    /**
     * Set the result of a delete operation, the object deleted from the tree if
     * any.
     * 
     * @param result
     *            The mutation result.
     */
    public void setResult(T result)
    {
        this.result = result;
    }
    
    /**
     * Get the value to use to replace an inner tier pivot if the pivot
     * value is going to be deleted entirely from the leaf.
     *  
     * @return The pivot replacement.
     */
    public T getReplacement()
    {
        return replacement;
    }
    
    /**
     * Set the value to use to replace an inner tier pivot if the pivot
     * value is going to be deleted entirely from the leaf.
     *  
     * @param replacement The pivot replacement.
     */
    public void setReplacement(T replacement)
    {
        this.replacement = replacement;
    }
    
    // TODO Document.
    public LeafTier<T, A> getLeftLeaf()
    {
        return leftLeaf;
    }
    
    // TODO Document.
    public void setLeftLeaf(LeafTier<T, A> leftLeaf)
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
    public InnerTier<T, A> newInnerTier(ChildType childType)
    {
        InnerTier<T, A> inner = new InnerTier<T, A>();
        inner.setAddress(getStructure().getAllocator().allocate(getStash(), inner, getStructure().getInnerSize()));
        inner.setChildType(childType);
        return inner;
    }
    
    // TODO Document.
    public LeafTier<T, A> newLeafTier()
    {
        LeafTier<T, A> leaf = new LeafTier<T, A>();
        leaf.setAddress(getStructure().getAllocator().allocate(getStash(), leaf, getStructure().getInnerSize()));
        return leaf;
    }
    
    // TODO Document.
    public void rewind(int leaveExclusive)
    {
        Iterator<Level<T, A>>levels = listOfLevels.iterator();
        int size = listOfLevels.size();
        boolean unlock = true;

        for (int i = 0; i < size - leaveExclusive; i++)
        {
            Level<T, A> level = levels.next();
            Iterator<Operation<T, A>> operations = level.listOfOperations.iterator();
            while (operations.hasNext())
            {
                Operation<T, A> operation = operations.next();
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
        Iterator<Level<T, A>> levels = listOfLevels.iterator();
        while (listOfLevels.size() > 3 && levels.hasNext())
        {
            Level<T, A> level = levels.next();
            if (level.listOfOperations.size() != 0)
            {
                break;
            }

            level.releaseAndClear();
            levels.remove();
        }
    }
    
    // TODO Document.
    public Comparable<T> _newComparable(T object)
    {
        return null;
    }
    
    // TODO Document.
    public void clear()
    {
        
    }
}