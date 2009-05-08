package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.link;

/**
 * Split a leaf by creating a new empty leaf to the left of a leaf that is part
 * of a linked list of b+tree leaves of duplicate index values and adding the leaf
 * value to the new leaf. The split is actually performed by creating a new
 * empty leaf to the right and copying the entire contents of the leaf to the
 * new empty leaf to the right. This is because the b+tree leaves are singly
 * linked, and linking a new leaf to the left is not possible without decending
 * the tree to find the leaf to the left.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class SplitLinkedListLeft<T, A>
implements LeafOperation<T, A>
{
    /** The inner tier parent of the leaf tier to split left. */
    private final InnerTier<T, A> inner;

    /**
     * Create a split left operation with the given inner tier parent.
     * 
     * @param inner
     *            The inner tier parent of the leaf tier to split left.
     */
    public SplitLinkedListLeft(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    /**
     * Perform the left leaf split and insert the value object.
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

        // Create a new leaf tier. It goes to the right of the current leaf
        // tier that is going to split left, so we copy the contents of the
        // splitting tier to the new tier. (We can only link to the right.)
        LeafTier<T, A> right = mutation.newLeafTier();
        while (leaf.size() != 0)
        {
            right.add(leaf.remove(0));
        }

        // Link the new right tier with the copied content to the right.
        link(mutation, leaf, right);

        // Replace the pivot for the leaf tier in the parent tier and add a
        // pivot for the new leaf tier to the right.
        int index = inner.getIndex(leaf.getAddress());
        inner.get(index).setPivot(mutation.getObject());
        inner.add(index + 1, new Branch<T, A>(right.get(0), right.getAddress()));

        // Stage the dirty tiers for write.
        Stage<T, A> stage = structure.getStage();
        stage.dirty(mutation.getStash(), inner);
        stage.dirty(mutation.getStash(), leaf);
        stage.dirty(mutation.getStash(), right);

        // Insert the object value.
        return new InsertSorted<T, A>(inner).operate(mutation, leafLevel);
    }
}
