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
 * <p>
 * Note that you considered creating byValue() and withComparable() interfaces
 * but decided that it was better to expose comparable() since when you define a
 * comparable factory, you are going to use only field values, when you don't,
 * you're using a comparable anyway. When you only use field values, you
 * need to build a comparable around those values, so withComparable() will
 * become extremely verbose.
 * 
 * @author Alan Gutierrez
 * 
 * @param <Record>
 *            The value type of the b+tree objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public final class CoreQuery<Record, Address>
implements Query<Record> {
    /** The type-safe container of out of band data. */
    private final Stash stash;
    
    /** The b+tree. */
    private final CoreStrata<Record, Address> strata;

    /** The collection of the core services of the b+tree. */
    private final Structure<Record, Address> structure;
    
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
    public CoreQuery(Stash stash, CoreStrata<Record, Address> strata, Structure<Record, Address> structure) {
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
    public Lock getInsertDeleteLock() {
        return structure.getStage().getInsertDeleteLock();
    }

    /**
     * Get the type-safe container of out of band data.
     * 
     * @return The type-safe container of out of band data.
     */
    public Stash getStash() {
        return stash;
    }
    
    /**
     * Get the b+tree.
     * 
     * @return The b+tree.
     */
    public Strata<Record> getStrata() {
        return strata;
    }

    /**
     * Get the root inner tier of the b+tree.
     * 
     * @return The root inner tier of the b+tree.
     */
    private Tier<Record, Address> getRoot() {
        return structure.getStorage().load(stash, strata.getRootAddress());
    }

    /**
     * Tests to see if the child is in a state where it will split or merge, or
     * whether the parent pivot is going to be deleted and needs to be swapped.
     * If so, the parent and the child are locked exclusively, if not already
     * locked exclusively. This method also rewinds split or merge operations of
     * the child is not going to split or merge.
     * 
     * @param mutation
     *            The mutation state container.
     * @param subequent
     *            A split or merge decision.
     * @param swap
     *            A swap key decision if deleting.
     * @param parentLevel
     *            The operations to perform on the parent tier.
     * @param parentLevel
     *            The operations to perform on the child tier.
     * @param parent
     *            The parent tier.
     * @param leaveExclusive
     *            The number of levels above and including the parent level to
     *            leave exclusive when rewinding.
     */
    private void testInnerTier(Mutation<Record, Address> mutation, Decision<Record, Address> subsequent, Decision<Record, Address> swap, Level<Record, Address> parentLevel, Level<Record, Address> childLevel, Tier<Record, Address> parent, int leaveExclusive) {
        // Test to see if the parent may be split or merged.
        boolean tiers = subsequent.test(mutation, parentLevel, childLevel, parent);

        // Test to see if the pivot value is a value about to deleted.
        boolean keys = swap.test(mutation, parentLevel, childLevel, parent);

        // If the parent needs to be locked exclusively, make it so.
        if (tiers || keys) {
            // We need to lock both the parent and child exclusive, so upgrade
            // the locks and try again if necessary. Otherwise, if this lock is
            // for swap only, we can rewind any splits or merges.
            if (!parentLevel.locker.isWrite() || !childLevel.locker.isWrite()) {
                parentLevel.upgrade(childLevel);
                parentLevel.operations.clear();
                childLevel.operations.clear();
                testInnerTier(mutation, subsequent, swap, parentLevel, childLevel, parent, leaveExclusive);
            } else if (!tiers) {
                mutation.rewind(leaveExclusive);
            }
        } else {
            // There are not splits or merged here, so we can discard any splits
            // or merges above this level.
            mutation.rewind(leaveExclusive);
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
    private Record generalized(Mutation<Record, Address> mutation, RootDecision<Record, Address> initial, Decision<Record, Address> subsequent, Decision<Record, Address> swap, Decision<Record, Address> penultimate) {
        Stage<Record, Address> stage = structure.getStage();

        mutation.levels.add(new Level<Record, Address>(false));

        Tier<Record, Address> parent = getRoot();
        Level<Record, Address> parentLevel = new Level<Record, Address>(false);
        parentLevel.lockAndAdd(parent);
        mutation.levels.add(parentLevel);

        Level<Record, Address> childLevel = new Level<Record, Address>(false);
        mutation.levels.add(childLevel);

        if (initial.test(mutation, parentLevel, parent)) {
            parentLevel.upgrade(childLevel);
            if (initial.test(mutation, parentLevel, parent)) {
                initial.operation(mutation, parentLevel, parent);
            } else {
                mutation.rewind(0);
            }
        }

        for (;;) {
            if (parent.isChildLeaf()) {
                testInnerTier(mutation, penultimate, swap, parentLevel, childLevel, parent, 1);
                break;
            }
            testInnerTier(mutation, subsequent, swap, parentLevel, childLevel, parent, 0);
            int branch = parent.find(mutation.getComparable());
            Tier<Record, Address> child = structure.getStorage().load(mutation.getStash(), parent.getChildAddress(branch));
            parent = child;
            parentLevel = childLevel;
            childLevel = new Level<Record, Address>(childLevel.locker.isWrite());
            mutation.levels.add(childLevel);
            mutation.shift();
        }

        if (mutation.leafOperation.operate(mutation, childLevel)) {
            ListIterator<Level<Record, Address>> levels = mutation.levels.listIterator(mutation.levels.size());
            while (levels.hasPrevious()) {
                Level<Record, Address> level = levels.previous();
                ListIterator<Operation<Record, Address>> operations = level.operations.listIterator(level.operations.size());
                while (operations.hasPrevious()) {
                    Operation<Record, Address> operation = operations.previous();
                    operation.operate(mutation);
                }
            }
        }

        ListIterator<Level<Record, Address>> levels = mutation.levels.listIterator(mutation.levels.size());
        while (levels.hasPrevious()) {
            Level<Record, Address> level = levels.previous();
            level.releaseAndClear();
        }
        
        stage.flush(stash, lockCount, false);
        stage.end(lockCount);

        return mutation.getResult();
    }

    /**
     * Add the given object to the b+tree.
     * 
     * @param object
     *            The object to add.
     */
    public void add(Record object) {
        Comparable<? super Record> fields = structure.getComparableFactory().newComparable(stash, object);
        Mutation<Record, Address> mutation = new Mutation<Record,Address>(stash, structure, fields, object, null);
        generalized(mutation, new ShouldSplitRoot<Record, Address>(), new ShouldSplitInner<Record, Address>(), new InnerNever<Record, Address>(), new HowToInertLeaf<Record, Address>());
    }

    /**
     * Build a comparable from the given value object using the comparable
     * factory property that provides the comparables used to order the b+tree.
     * 
     * @return A comparable built from the given object according to the order
     *         of the b+tree.
     */
    public Comparable<? super Record> comparable(Record object) {
        return structure.getComparableFactory().newComparable(stash, object);
    }

    /**
     * Return a forward cursor that references if the first object value in the
     * b+tree that is less than or equal to the given comparable.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return A forward cursor that references the first object value in the
     *         b+tree that is less than or equal to the given comparable.
     */
    public Cursor<Record> find(Comparable<? super Record> fields) {
        Lock previous = new ReentrantLock();
        previous.lock();
        Tier<Record, Address> inner = getRoot();
    
        for (;;) {
            inner.readWriteLock.readLock().lock();
            previous.unlock();
            previous = inner.readWriteLock.readLock();
            int branch = inner.find(fields);
            if (inner.isChildLeaf()) {
                Tier<Record, Address> leaf = structure.getStorage().load(stash, inner.getChildAddress(branch));
                leaf.readWriteLock.readLock().lock();
                previous.unlock();
                return new CoreCursor<Record, Address>(stash, structure, leaf, leaf.find(fields));
            }
            inner = structure.getStorage().load(stash, inner.getChildAddress(branch));
        }
    }

    /**
     * Constructs an instance of deletable that will always return true. This
     * deletable is used to remove the first stored value whose fields match the
     * comparable passed to remove.
     * 
     * @return An instance of deletable that will always return true.
     */
    private Deletable<Record> deleteAny() {
        return new Deletable<Record>() {
            public boolean deletable(Record object) {
                return true;
            }
        };
    }

    /**
     * Remove the first object that is equal to the given comparable that is
     * deletable according to the the given deletable if any.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return The removed object or null if no object is both equal to the
     *         given comparable and deletable according to the given deletable.
     */
    public Record remove(Deletable<Record> deletable, Comparable<? super Record> comparable) {
        Mutation<Record, Address> mutation = new Mutation<Record, Address>(stash, structure, comparable, null, deletable);
        do {
            mutation.levels.clear();
            mutation.clear();
            generalized(mutation, new ShouldFillRoot<Record, Address>(), new ShouldMergeInner<Record, Address>(), new ShouldSwapKey<Record, Address>(), new HowToRemoveLeaf<Record, Address>());
        } while (mutation.isOnlyChild());

        return mutation.getResult();
    }

    /**
     * Return a forward cursor that references if the first object value in the
     * b+tree that is less than or equal to the given comparable.
     * 
     * @param comparable
     *            The comparable representing the value to find.
     * @return A forward cursor that references the first object value in the
     *         b+tree that is less than or equal to the given comparable.
     */
    public Record remove(Comparable<? super Record> comparable) {
        return remove(deleteAny(), comparable);
    }

    /**
     * Returns a cursor that references the first object in the b-tree.
     * 
     * @return A cursor that references the first object in the b-tree.
     */
    public Cursor<Record> first() {
        return null;
    }

    /**
     * Flush any dirty tiers held by in memory and guarded by the insert and
     * delete lock.
     */
    public void flush() {
        structure.getStage().flush(stash, lockCount, true);
    }

    /**
     * Destroy the b+tree by deallocating all of its pages from the persistent
     * storage including the root page.
     */
    public void destroy() {
    }
}
