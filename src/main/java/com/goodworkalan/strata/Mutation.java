package com.goodworkalan.strata;

import java.util.Iterator;
import java.util.LinkedList;

import com.goodworkalan.stash.Stash;

/**
 * The mutation state container. Contains the properties of a generalized
 * mutation.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class Mutation<T, A>
{
    /** The type-safe container of out of band data. */
    private final Stash stash;

    /** The collection of the core services of the b+tree. */
    private final Structure<T, A> structure;
    
    /** The comparable representing the value to find. */
    private final Comparable<? super T> comparable;
    
    /** The object being inserted. */
    private final T object;
    
    /**
     * The condition to test to determine if an object that matches the deletable
     * comparable should in fact be deleted.
     */
    final Deletable<T> deletable;
    
    /**
     * A linked list of the per level mutation state.
     */
    final LinkedList<Level<T, A>> levels = new LinkedList<Level<T, A>>();
    
    // TODO Document.
    private boolean onlyChild;
    
    // TODO Document.
    private boolean deleting;

    /**
     * The result of a delete operation, the object deleted from the tree if
     * any.
     */
    private T result;
    
    /**
     * The value to use to replace an inner tier pivot whose is being deleted
     * from b-tree.
     */
    private T replacement;
    
    // TODO Document.
    private LeafTier<T, A> leftLeaf;
    
    /** The leaf operation to perform. */
    public LeafOperation<T, A> leafOperation;

    /**
     * Create a new mutation.
     * 
     * @param stash
     *            The type-safe container of out of band data.
     * @param structure
     *            The collection of the core services of the b+tree.
     * @param comparable
     *            The comparable representing the value to find.
     * @param object
     *            The object being inserted.
     * @param deletable
     *            The condition to test to determine if an object that matches
     *            the deletable comparable should in fact be deleted.
     */
    public Mutation(Stash stash, Structure<T, A> structure, Comparable<? super T> comparable, T object, Deletable<T> deletable)
    {
        this.stash = stash;
        this.comparable = comparable;
        this.deletable = deletable;
        this.object = object;
        this.structure = structure;
    }

    /**
     * Get the comparable representing the value to find.
     * 
     * @return The comparable representing the value to find.
     */
    public Comparable<? super T> getComparable()
    {
        return comparable;
    }

    /**
     * Get the type-safe container of out of band data.
     * 
     * @return The type-safe container of out of band data.
     */
    public Stash getStash()
    {
        return stash;
    }

    /**
     * Get the object being inserted.
     * 
     * @return The object being inserted.
     */
    public T getObject()
    {
        return object;
    }

    /**
     * Get the collection of the core services of the b+tree.
     * 
     * @return The collection of the core services of the b+tree.
     */
    public Structure<T, A> getStructure()
    {
        return structure;
    }

    /**
     * Get the result of a delete operation, the object deleted from the tree if
     * any.
     * 
     * @return The delete result.
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
     *            The delete result.
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

    /**
     * Create a new inner tier that references child tiers of the given child
     * type.
     * 
     * @param childType
     *            The child tier type.
     * @return A new inner tier.
     */
    public InnerTier<T, A> newInnerTier(ChildType childType)
    {
        InnerTier<T, A> inner = new InnerTier<T, A>();
        inner.setAddress(getStructure().getAllocator().allocate(getStash(), inner, getStructure().getInnerSize()));
        inner.setChildType(childType);
        return inner;
    }

    /**
     * Create a new leaf tier.
     * 
     * @return A new leaf tier.
     */
    public LeafTier<T, A> newLeafTier()
    {
        LeafTier<T, A> leaf = new LeafTier<T, A>();
        leaf.setAddress(getStructure().getAllocator().allocate(getStash(), leaf, getStructure().getInnerSize()));
        return leaf;
    }
    
    // TODO Document.
    public void rewind(int leaveExclusive)
    {
        Iterator<Level<T, A>> eachLevel = levels.iterator();
        int size = levels.size();
        boolean unlock = true;

        for (int i = 0; i < size - leaveExclusive; i++)
        {
            Level<T, A> level = eachLevel.next();
            Iterator<Operation<T, A>> operations = level.operations.iterator();
            while (operations.hasNext())
            {
                Operation<T, A> operation = operations.next();
                if (operation.isSplitOrMerge())
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
                if (levels.size() == 3)
                {
                    level.downgrade();
                }
                else
                {
                    level.releaseAndClear();
                    eachLevel.remove();
                }
            }
        }
    }

    // TODO Document.
    public void shift()
    {
        Iterator<Level<T, A>> eachLevel = levels.iterator();
        while (levels.size() > 3 && eachLevel.hasNext())
        {
            Level<T, A> level = eachLevel.next();
            if (level.operations.size() != 0)
            {
                break;
            }

            level.releaseAndClear();
            eachLevel.remove();
        }
    }
    
    // TODO Document.
    public void clear()
    {
    }
}