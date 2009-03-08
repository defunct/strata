package com.goodworkalan.strata;

import java.util.ListIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.stash.Stash;

/**
 * The core implementation of the query interface.
 * <p>
 * This is an interface/implementation because the address type parameter does
 * not need to be exposed through the query interface.
 * <p>
 * TODO Consider an API change where you have separate interfaces for seraching
 * by value or by comparable. Would you use it? Where do you use newComparable?
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class CoreQuery<T, A>
implements Query<T>
{
    /** The type-safe container of out of band data. */
    private final Stash stash;
    
    /** The b+tree. */
    private final CoreStrata<T, A> strata;

    /** The collection of the core services of the b+tree. */
    private final Structure<T, A> structure;
    
    /**
     * The count of times that the exclusive insert or delete lock was locked
     * by begin and then not unlocked by end because there were dirty pages
     * outstanding.
     */
    private int[] lockCount;

    /**
     * Create a new query implementation.
     * 
     * @param stash
     *            The type-safe container of out of band data.
     * @param strata
     *            The b+tree.
     * @param structure
     *            The collection of the core services of the b+tree
     */
    public CoreQuery(Stash stash, CoreStrata<T, A> strata, Structure<T, A> structure)
    {
        this.stash = stash;
        this.strata = strata;
        this.structure = structure;
        this.lockCount = new int[1];
    }

    
    /**
     * Get the lock that locks the b+tree exclusively for insert and update.
     * 
     * @return The insert delete lock.
     */
    public Lock getInsertDeleteLock()
    {
        return structure.getStage().getInsertDeleteLock();
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
     * Get the b+tree.
     * 
     * @return The b+tree.
     */
    public Strata<T> getStrata()
    {
        return strata;
    }

    /**
     * Get the root inner tier of the b+tree.
     * 
     * @return The root inner tier of the b+tree.
     */
    private InnerTier<T, A> getRoot()
    {
        return structure.getPool().getInnerTier(stash, strata.getRootAddress());
    }

    // TODO Document.
    private void testInnerTier(Mutation<T, A> mutation, Decision<T, A> subsequent, Decision<T, A> swap, Level<T, A> levelOfParent, Level<T, A> levelOfChild, InnerTier<T, A> parent, int rewind)
    {
        boolean tiers = subsequent.test(mutation, levelOfParent, levelOfChild, parent);
        boolean keys = swap.test(mutation, levelOfParent, levelOfChild, parent);
        if (tiers || keys)
        {
            if (!levelOfParent.getSync.isExeclusive() || !levelOfChild.getSync.isExeclusive())
            {
                levelOfParent.upgrade(levelOfChild);
                levelOfParent.listOfOperations.clear();
                levelOfChild.listOfOperations.clear();
                testInnerTier(mutation, subsequent, swap, levelOfParent, levelOfChild, parent, rewind);
            }
            else if (!tiers)
            {
                mutation.rewind(rewind);
            }
        }
        else
        {
            mutation.rewind(rewind);
        }
    }

    /**
     * Both {@link #insert inert()} and {@link #remove remove()} use this
     * generalized mutation method that implements locking the proper tiers
     * during the descent of the tree to find the leaf to mutate.
     * <p>
     * This generalized mutation will insert or remove a single item.
     * 
     * @param mutation
     *            An object that maintains the state of this insert or delete.
     * @param initial
     *            A decision to split or merge the root.
     * @param subsequent
     *            A decision to split, merge or delete an inner tier that is not
     *            the root tier.
     * @param swap
     *            For remove, determine if the object removed is an inner tier
     *            pivot and needs to be swapped.
     * @param penultimate
     *            A decision about the both the inner tier that references
     *            leaves and the leaf tier itself, whether to split, merge or
     *            delete the leaf, the insert or delete action to take on the
     *            leaf, or whether to restart the descent.
     */
    private T generalized(Mutation<T, A> mutation, RootDecision<T, A> initial, Decision<T, A> subsequent, Decision<T, A> swap, Decision<T, A> penultimate)
    {
        structure.getStage().begin();

        mutation.listOfLevels.add(new Level<T, A>(false));

        InnerTier<T, A> parent = getRoot();
        Level<T, A> levelOfParent = new Level<T, A>(false);
        levelOfParent.lockAndAdd(parent);
        mutation.listOfLevels.add(levelOfParent);

        Level<T, A> levelOfChild = new Level<T, A>(false);
        mutation.listOfLevels.add(levelOfChild);

        if (initial.test(mutation, levelOfParent, parent))
        {
            levelOfParent.upgrade(levelOfChild);
            if (initial.test(mutation, levelOfParent, parent))
            {
                initial.operation(mutation, levelOfParent, parent);
            }
            else
            {
                mutation.rewind(0);
            }
        }

        for (;;)
        {
            if (parent.getChildType() == ChildType.INNER)
            {
                testInnerTier(mutation, subsequent, swap, levelOfParent, levelOfChild, parent, 0);
                Branch<T, A> branch = parent.find(mutation.getComparable());
                InnerTier<T, A> child = structure.getPool().getInnerTier(mutation.getStash(), branch.getAddress());
                parent = child;
            }
            else
            {
                testInnerTier(mutation, penultimate, swap, levelOfParent, levelOfChild, parent, 1);
                break;
            }
            levelOfParent = levelOfChild;
            levelOfChild = new Level<T, A>(levelOfChild.getSync.isExeclusive());
            mutation.listOfLevels.add(levelOfChild);
            mutation.shift();
        }

        if (mutation.leafOperation.operate(mutation, levelOfChild))
        {
            ListIterator<Level<T, A>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level<T, A> level = levels.previous();
                ListIterator<Operation<T, A>> operations = level.listOfOperations.listIterator(level.listOfOperations.size());
                while (operations.hasPrevious())
                {
                    Operation<T, A> operation = operations.previous();
                    operation.operate(mutation);
                }
            }
        }

        ListIterator<Level<T, A>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
        while (levels.hasPrevious())
        {
            Level<T, A> level = levels.previous();
            level.releaseAndClear();
        }
        
        structure.getStage().flush(stash, lockCount, false);
        structure.getStage().end(lockCount);

        return mutation.getResult();
    }

    /**
     * Add the given object to the b+tree.
     * 
     * @param object
     *            The object to add.
     */
    public void add(T object)
    {
        Comparable<? super T> fields = structure.getComparableFactory().newComparable(stash, object);
        Mutation<T, A> mutation = new Mutation<T,A>(stash, structure, fields, object, null);
        generalized(mutation, new ShouldSplitRoot<T, A>(), new ShouldSplitInner<T, A>(), new InnerNever<T, A>(), new HowToInertLeaf<T, A>());
    }
    
    // TODO Document.
    public Comparable<? super T> newComparable(T object)
    {
        return structure.getComparableFactory().newComparable(stash, object);
    }

    // TODO Document.
    public Cursor<T> find(Comparable<? super T> fields)
    {
        Lock previous = new ReentrantLock();
        previous.lock();
        InnerTier<T, A> inner = getRoot();
    
        for (;;)
        {
            inner.getReadWriteLock().readLock().lock();
            previous.unlock();
            previous = inner.getReadWriteLock().readLock();
            Branch<T, A> branch = inner.find(fields);
            if (inner.getChildType() == ChildType.LEAF)
            {
                LeafTier<T, A> leaf = structure.getPool().getLeafTier(stash, branch.getAddress());
                leaf.getReadWriteLock().readLock().lock();
                previous.unlock();
                return new CoreCursor<T, A>(stash, structure, leaf, leaf.find(fields));
            }
            inner = structure.getPool().getInnerTier(stash, branch.getAddress());
        }
    }

    /**
     * Constructs an instance of deletable that will always return true. This
     * deletable is used to remove the first stored value whose fields match the
     * comparable passed to remove.
     * 
     * @return An instance of deletable that will always return true.
     */
    private Deletable<T> deleteAny()
    {
        return new Deletable<T>()
        {
            public boolean deletable(T object)
            {
                return true;
            }
        };
    }

    // TODO Document.
    public T remove(Deletable<T> deletable, Comparable<? super T> comparable)
    {
        Mutation<T, A> mutation = new Mutation<T, A>(stash, structure, comparable, null, deletable);
        do
        {
            mutation.listOfLevels.clear();
            mutation.clear();
            generalized(mutation, new ShouldDeleteRoot<T, A>(), new ShouldMergeInner<T, A>(), new ShouldSwapKey<T, A>(), new HowToRemoveLeaf<T, A>());
        }
        while (mutation.isOnlyChild());

        return mutation.getResult();
    }

    // TODO Document.
    public T remove(Comparable<? super T> comparable)
    {
        return remove(deleteAny(), comparable);
    }
    
    // TODO Document.
    public Cursor<T> first()
    {
        return null;
    }
    
    // TODO Document.
    public void flush()
    {
        structure.getStage().flush(stash, lockCount, true);
    }

    // TODO Document.
    public void destroy()
    {
    }
}
