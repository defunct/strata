package com.goodworkalan.strata;

import java.util.ListIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.goodworkalan.stash.Stash;

public final class CoreQuery<B, T, F extends Comparable<? super F>, A>
implements Query<T, F>
{
    private final Stash stash;
    
    private final CoreTree<B, T, F, A> strata;

    private final Structure<B, A> structure;

    public CoreQuery(Stash stash, CoreTree<B, T, F, A> strata, Structure<B, A> structure)
    {
        this.stash = stash;
        this.strata = strata;
        this.structure = structure;
    }

    public Stash getStash()
    {
        return stash;
    }
    
    public Strata<T, F> getStrata()
    {
        return strata;
    }

    private InnerTier<B, A> getRoot()
    {
        return structure.getPool().getInnerTier(stash, strata.getRootAddress());
    }

    private void testInnerTier(Mutation<B, A> mutation,
            Decision<B, A> subsequent, Decision<B, A> swap,
            Level<B, A> levelOfParent, Level<B, A> levelOfChild,
            InnerTier<B, A> parent, int rewind)
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
    private B generalized(Mutation<B, A> mutation,
            RootDecision<B, A> initial, Decision<B, A> subsequent,
            Decision<B, A> swap, Decision<B, A> penultimate)
    {
        // TODO Replace this with our caching pattern.

        // Inform the tier cache that we are about to perform a mutation
        // of the tree.
        mutation.getStructure().getWriter().begin();

        mutation.listOfLevels.add(new Level<B, A>(false));

        InnerTier<B, A> parent = getRoot();
        Level<B, A> levelOfParent = new Level<B, A>(false);
        levelOfParent.lockAndAdd(parent);
        mutation.listOfLevels.add(levelOfParent);

        Level<B, A> levelOfChild = new Level<B, A>(false);
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
                Branch<B, A> branch = parent.find(mutation.getComparable());
                InnerTier<B, A> child = structure.getPool().getInnerTier(mutation.getTxn(), branch.getAddress());
                parent = child;
            }
            else
            {
                testInnerTier(mutation, penultimate, swap, levelOfParent, levelOfChild, parent, 1);
                break;
            }
            levelOfParent = levelOfChild;
            levelOfChild = new Level<B, A>(levelOfChild.getSync.isExeclusive());
            mutation.listOfLevels.add(levelOfChild);
            mutation.shift();
        }

        if (mutation.leafOperation.operate(mutation, levelOfChild))
        {
            ListIterator<Level<B, A>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level<B, A> level = levels.previous();
                ListIterator<Operation<B, A>> operations = level.listOfOperations.listIterator(level.listOfOperations.size());
                while (operations.hasPrevious())
                {
                    Operation<B, A> operation = operations.previous();
                    operation.operate(mutation);
                }
            }

            // FIXME Probably does not belong here?
            mutation.getStructure().getWriter().end(mutation.getTxn());
        }

        ListIterator<Level<B, A>> levels = mutation.listOfLevels.listIterator(mutation.listOfLevels.size());
        while (levels.hasPrevious())
        {
            Level<B, A> level = levels.previous();
            level.releaseAndClear();
        }

        return mutation.getResult();
    }

    public void add(T object)
    {
        F fields = strata.getExtractor().extract(stash, object);
        B bucket = strata.getCooper().newBucket(fields, object);
        BucketComparable<T, F, B> comparable = new BucketComparable<T, F, B>(stash, strata.getCooper(), strata.getExtractor(), fields);
        Mutation<B, A> mutation = new Mutation<B, A>(stash, structure, bucket, comparable, null);
        generalized(mutation, new SplitRoot<B, A>(), new SplitInner<B, A>(), new InnerNever<B, A>(), new LeafInsert<B, A>());
    }

    public Deletable<T> deleteAny()
    {
        return new Deletable<T>()
        {
            public boolean deletable(T object)
            {
                return true;
            }
        };
    }

    // TODO Where do I actually use deletable? Makes sense, though. A
    // condition to choose which to delete.
    public T remove(Deletable<T> deletable, Comparable<? super F> comparable)
    {
        BucketComparable<T, F, B> bucketComparable = new BucketComparable<T, F, B>(stash, strata.getCooper(), strata.getExtractor(), comparable);
        Mutation<B, A> mutation = new Mutation<B, A>(stash, structure, null, bucketComparable, new BucketDeletable<T, F, B>(strata.getCooper(), deletable));
        do
        {
            mutation.listOfLevels.clear();

            mutation.clear();

            generalized(mutation, new DeleteRoot<B, A>(),
                    new MergeInner<B, A>(), new SwapKey<B, A>(),
                    new LeafRemove<B, A>());
        }
        while (mutation.isOnlyChild());

        B removed = mutation.getResult();

        return strata.getCooper().getObject(removed);
    }

    public T remove(Comparable<? super F> comparable)
    {
        return remove(deleteAny(), comparable);
    }
    
    // Here is where I get the power of not using comparator.
    public Cursor<T> find(Comparable<? super F> fields)
    {
        Lock previous = new ReentrantLock();
        previous.lock();
        InnerTier<B, A> inner = getRoot();
       
        Comparable<B> comparator = new BucketComparable<T, F, B>(stash, strata.getCooper(), strata.getExtractor(), fields);
        for (;;)
        {
            inner.getReadWriteLock().readLock().lock();
            previous.unlock();
            previous = inner.getReadWriteLock().readLock();
            Branch<B, A> branch = inner.find(comparator);
            if (inner.getChildType() == ChildType.LEAF)
            {
                LeafTier<B, A> leaf = structure.getPool().getLeafTier(stash, branch.getAddress());
                leaf.getReadWriteLock().readLock().lock();
                previous.unlock();
                Cursor<B> cursor = new CoreCursor<B, A>(stash, structure, leaf, leaf.find(comparator));
                return strata.getCooper().wrap(cursor);
            }
            inner = structure.getPool().getInnerTier(stash, branch.getAddress());
        }
    }

    public Cursor<T> first()
    {
        return null;
    }

    public F extract(T object)
    {
        return strata.getExtractor().extract(stash, object);
    }
    
    public void flush()
    {
    }
    
    public void destroy()
    {
    }
}
