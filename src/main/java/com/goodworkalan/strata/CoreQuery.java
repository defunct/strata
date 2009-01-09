package com.goodworkalan.strata;

import java.util.ListIterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class CoreQuery<B, T, F extends Comparable<F>, A, X> implements
        Transaction<T, F, X>
{
    private final X txn;

    private final CoreTree<B, T, F, A, X> tree;

    private final Structure<B, A, X> structure;

    public CoreQuery(X txn, CoreTree<B, T, F, A, X> tree,
            Structure<B, A, X> structure)
    {
        this.txn = txn;
        this.tree = tree;
        this.structure = structure;
    }

    public Strata<T, F, X> getStrata()
    {
        return tree;
    }

    public X getState()
    {
        return txn;
    }

    private InnerTier<B, A> getRoot()
    {
        return structure.getPool().getInnerTier(txn, tree.getRootAddress());
    }

    private void testInnerTier(Mutation<B, A, X> mutation,
            Decision<B, A, X> subsequent, Decision<B, A, X> swap,
            Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild,
            InnerTier<B, A> parent, int rewind)
    {
        boolean tiers = subsequent.test(mutation, levelOfParent, levelOfChild,
                parent);
        boolean keys = swap.test(mutation, levelOfParent, levelOfChild, parent);
        if (tiers || keys)
        {
            if (!levelOfParent.getSync.isExeclusive()
                    || !levelOfChild.getSync.isExeclusive())
            {
                levelOfParent.upgrade(levelOfChild);
                levelOfParent.listOfOperations.clear();
                levelOfChild.listOfOperations.clear();
                testInnerTier(mutation, subsequent, swap, levelOfParent,
                        levelOfChild, parent, rewind);
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
    private B generalized(Mutation<B, A, X> mutation,
            RootDecision<B, A, X> initial, Decision<B, A, X> subsequent,
            Decision<B, A, X> swap, Decision<B, A, X> penultimate)
    {
        // TODO Replace this with our caching pattern.

        // Inform the tier cache that we are about to perform a mutation
        // of the tree.
        mutation.getStructure().getWriter().begin();

        mutation.listOfLevels.add(new Level<B, A, X>(false));

        InnerTier<B, A> parent = getRoot();
        Level<B, A, X> levelOfParent = new Level<B, A, X>(false);
        levelOfParent.lockAndAdd(parent);
        mutation.listOfLevels.add(levelOfParent);

        Level<B, A, X> levelOfChild = new Level<B, A, X>(false);
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
                testInnerTier(mutation, subsequent, swap, levelOfParent,
                        levelOfChild, parent, 0);
                Branch<B, A> branch = parent.find(mutation.getComparable());
                InnerTier<B, A> child = structure.getPool().getInnerTier(
                        mutation.getTxn(), branch.getAddress());
                parent = child;
            }
            else
            {
                testInnerTier(mutation, penultimate, swap, levelOfParent,
                        levelOfChild, parent, 1);
                break;
            }
            levelOfParent = levelOfChild;
            levelOfChild = new Level<B, A, X>(levelOfChild.getSync
                    .isExeclusive());
            mutation.listOfLevels.add(levelOfChild);
            mutation.shift();
        }

        if (mutation.leafOperation.operate(mutation, levelOfChild))
        {
            ListIterator<Level<B, A, X>> levels = mutation.listOfLevels
                    .listIterator(mutation.listOfLevels.size());
            while (levels.hasPrevious())
            {
                Level<B, A, X> level = levels.previous();
                ListIterator<Operation<B, A, X>> operations = level.listOfOperations
                        .listIterator(level.listOfOperations.size());
                while (operations.hasPrevious())
                {
                    Operation<B, A, X> operation = operations.previous();
                    operation.operate(mutation);
                }
            }

            mutation.getStructure().getWriter().end(mutation.getTxn()); // FIXME
                                                                        // Probably
                                                                        // does
                                                                        // not
                                                                        // belong
                                                                        // here?
        }

        ListIterator<Level<B, A, X>> levels = mutation.listOfLevels
                .listIterator(mutation.listOfLevels.size());
        while (levels.hasPrevious())
        {
            Level<B, A, X> level = levels.previous();
            level.releaseAndClear();
        }

        return mutation.getResult();
    }

    public void add(T object)
    {
        F fields = tree.getExtractor().extract(txn, object);
        B bucket = tree.getCooper().newBucket(fields, object);
        BucketComparable<T, F, B, X> comparable = new BucketComparable<T, F, B, X>(
                txn, tree.getCooper(), tree.getExtractor(), fields);
        Mutation<B, A, X> mutation = new Mutation<B, A, X>(txn, structure,
                bucket, comparable, null);
        generalized(mutation, new SplitRoot<B, A, X>(),
                new SplitInner<B, A, X>(), new InnerNever<B, A, X>(),
                new LeafInsert<B, A, X>());
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
    public T remove(Deletable<T> deletable, F fields)
    {
        BucketComparable<T, F, B, X> comparable = new BucketComparable<T, F, B, X>(
                txn, tree.getCooper(), tree.getExtractor(), fields);
        Mutation<B, A, X> mutation = new Mutation<B, A, X>(txn, structure,
                null, comparable, new BucketDeletable<T, F, B, X>(tree
                        .getCooper(), deletable));
        do
        {
            mutation.listOfLevels.clear();

            mutation.clear();

            generalized(mutation, new DeleteRoot<B, A, X>(),
                    new MergeInner<B, A, X>(), new SwapKey<B, A, X>(),
                    new LeafRemove<B, A, X>());
        }
        while (mutation.isOnlyChild());

        B removed = mutation.getResult();

        return tree.getCooper().getObject(removed);
    }

    public T remove(F fields)
    {
        return remove(deleteAny(), fields);
    }

    // Here is where I get the power of not using comparator.
    public Cursor<T> find(F fields)
    {
        Lock previous = new ReentrantLock();
        previous.lock();
        InnerTier<B, A> inner = getRoot();
        Comparable<B> comparator = new BucketComparable<T, F, B, X>(txn, tree
                .getCooper(), tree.getExtractor(), fields);
        for (;;)
        {
            inner.getReadWriteLock().readLock().lock();
            previous.unlock();
            previous = inner.getReadWriteLock().readLock();
            Branch<B, A> branch = inner.find(comparator);
            if (inner.getChildType() == ChildType.LEAF)
            {
                LeafTier<B, A> leaf = structure.getPool().getLeafTier(txn,
                        branch.getAddress());
                leaf.getReadWriteLock().readLock().lock();
                previous.unlock();
                Cursor<B> cursor = new CoreCursor<B, A, X>(txn, structure,
                        leaf, leaf.find(comparator));
                return tree.getCooper().wrap(cursor);
            }
            inner = structure.getPool().getInnerTier(txn, branch.getAddress());
        }
    }

    public Cursor<T> first()
    {
        return null;
    }

    public F extract(T object)
    {
        return tree.getExtractor().extract(txn, object);
    }
    
    public void flush()
    {
    }
}