package com.goodworkalan.strata;

/**
 * Contains static methods for leaf manipulation common to both leaf tier and
 * inner tier operations.
 * 
 * @author Alan Gutierrez
 */
public class Leaves
{
    /**
     * Get the next leaf in the b-tree from the next property of the given leaf,
     * lock it and add it to the list of locked leaves in the given leaf level.
     * 
     * @param <T>
     *            The value type of the b+tree objects.
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param mutation
     *            The mutation state container.
     * @param leaf
     *            The leaf.
     * @param leafLevel
     *            The mutation state for the leaf level.
     * @return The next leaf or null if the given leaf is the last leaf in the
     *         b-tree.
     */
    public static <T, A> LeafTier<T, A> getNextAndLock(Mutation<T, A> mutation, LeafTier<T, A> leaf, Level<T, A> leafLevel)
    {
        Structure<T, A> structure = mutation.getStructure();
        if (!structure.getStorage().isNull(leaf.getNext()))
        {
            LeafTier<T, A> next = structure.getPool().getLeafTier(mutation.getStash(), leaf.getNext());
            leafLevel.lockAndAdd(next);
            return next;
        }
        return null;
    }

    /**
     * Link the given next leaf after the given leaf.
     * 
     * @param <T>
     *            The value type of the b+tree objects.
     * @param <A>
     *            The address type used to identify an inner or leaf tier.
     * @param mutation
     *            The mutation state container.
     * @param leaf
     *            The leaf.
     * @param nextLeaf
     *            The next leaf.
     */
    public static <T, A> void link(Mutation<T, A> mutation, LeafTier<T, A> leaf, LeafTier<T, A> nextLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();
        Stage<T, A> writer = structure.getStage();
        writer.dirty(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), nextLeaf);
        nextLeaf.setNext(leaf.getNext());
        leaf.setNext(nextLeaf.getAddress());
    }
}
