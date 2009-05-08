package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.getNextAndLock;
import static com.goodworkalan.strata.Leaves.link;

import com.goodworkalan.stash.Stash;

/**
 * Split a leaf by creating a new empty leaf to the right of a linked list of
 * b+tree leaves of duplicate index values and adding the leaf value to the new
 * leaf.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
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
        Storage<T, A> alloator = mutation.getStructure().getStorage();
        if (alloator.isNull(leaf.getNext()))
        {
            return true;
        }
        Stash stash = mutation.getStash();
        LeafTier<T, A> next = structure.getPool().getLeafTier(stash, leaf.getNext());
        return structure.getComparableFactory().newComparable(stash, leaf.get(0)).compareTo(next.get(0)) != 0;
    }

    /**
     * Perform the right leaf split and insert the value object.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leafLevel
     *            The per level mutation state for the leaf level.
     * @return True of the operation succeeded.
     */
    public boolean operate(Mutation<T, A> mutation, Level<T, A> leafLevel)
    {
        // Get the collection of the core services of the b+tree.
        Structure<T, A> structure = mutation.getStructure();

        // Find the branch that navigates to the leaf child.
        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        // Navigate to the end of the linked list of a linked list of b+tree
        // leaves of duplicate index values.
        LeafTier<T, A> last = leaf;
        while (!endOfList(mutation, last))
        {
            last = getNextAndLock(mutation, last, leafLevel);
        }

        // Create a new leaf and link it to the right of the leaf.
        LeafTier<T, A> right = mutation.newLeafTier();
        link(mutation, last, right);

        // Add a branch for the the new leaf in the parent inner tier. 
        inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<T, A>(mutation.getObject(), right.getAddress()));

        // Stage the dirty tiers for write.
        Stage<T, A> writer = structure.getStage();
        writer.dirty(mutation.getStash(), inner);
        writer.dirty(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), right);

        // Insert the object value.
        return new InsertSorted<T, A>(inner).operate(mutation, leafLevel);
    }
}
