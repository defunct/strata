package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.getNextAndLock;
import static com.goodworkalan.strata.Leaves.link;

import com.goodworkalan.stash.Stash;

// TODO Document.
final class SplitLinkedListRight<T, A>
implements LeafOperation<T, A>
{
    /** The inner tier parent that references the leaf to split. */
    private final InnerTier<T, A> inner;

    /**
     * Create a split linked list right operation.
     * 
     * @param inner
     *            The inner tier parent that references the leaf to split.
     */
    public SplitLinkedListRight(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    /**
     * Return true if the given leaf is the last leaf in a linked list of leaves
     * containing object with identical index values.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leaf
     *            The leaf.
     * @return True if the given leaf is the last leaf in a linked list of
     *         leaves.
     */
    private boolean endOfList(Mutation<T, A> mutation, LeafTier<T, A> leaf)
    {
        Structure<T, A> structure = mutation.getStructure();
        Allocator<T, A> alloator = mutation.getStructure().getAllocator();
        if (alloator.isNull(leaf.getNext()))
        {
            return true;
        }
        Stash stash = mutation.getStash();
        LeafTier<T, A> next = structure.getPool().getLeafTier(stash, leaf.getNext());
        return structure.getComparableFactory().newComparable(stash, leaf.get(0)).compareTo(next.get(0)) != 0;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();

        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        LeafTier<T, A> last = leaf;
        while (!endOfList(mutation, last))
        {
            last = getNextAndLock(mutation, last, levelOfLeaf);
        }

        LeafTier<T, A> right = mutation.newLeafTier();
        link(mutation, last, right);

        inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<T, A>(mutation.getObject(), right.getAddress()));

        Stage<T, A> writer = structure.getStage();
        writer.dirty(mutation.getStash(), inner);
        writer.dirty(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), right);

        return new InsertSorted<T, A>(inner).operate(mutation, levelOfLeaf);
    }
}