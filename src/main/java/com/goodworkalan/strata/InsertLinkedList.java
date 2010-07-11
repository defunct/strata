package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.getNextAndLock;
import static com.goodworkalan.strata.Leaves.link;

/**
 * Append the mutation value object property to linked list of leaves containing
 * object with identical index values that begins at the leaf property.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class InsertLinkedList<T, A>
implements LeafOperation<T, A> {
    /**
     * A leaf that is the head of a linked list of value objects with identical
     * index values.
     */            
    private final LeafTier<T, A> leaf;

    /**
     * Create an insert linked list operation.
     * 
     * @param leaf
     *            A leaf that is the head of a linked list of value objects with
     *            identical index values.
     */
    public InsertLinkedList(LeafTier<T, A> leaf) {
        this.leaf = leaf;
    }

    /**
     * Append the mutation value object property to linked list of leaves
     * containing object with identical index values that begins at the given
     * leaf.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leafLevel
     *            The operations to perform on the leaf tier.
     * @return True indicating that the operation was successful.
     */
    public boolean operate(Mutation<T, A> mutation, Level<T, A> leafLevel) {
        append(mutation, leaf, leafLevel);
        return true;
    }

    /**
     * Append the mutation value object property to linked list of leaves
     * containing object with identical index values that begins at the given
     * leaf.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leaf
     *            A leaf that is the head of a linked list of value objects with
     *            identical index values.
     * @param leafLevel
     *            The operations to perform on the leaf tier.
     */
    private void append(Mutation<T, A> mutation, LeafTier<T, A> leaf, Level<T, A> leafLevel) {
        Structure<T, A> structure = mutation.getStructure();
        if (leaf.size() == structure.getLeafSize()) {
            LeafTier<T, A> nextLeaf = getNextAndLock(mutation, leaf, leafLevel);
            if (null == nextLeaf || structure.getComparableFactory().newComparable(mutation.getStash(), mutation.getObject()).compareTo(nextLeaf.get(0)) != 0) {
                nextLeaf = mutation.newLeafTier();
                link(mutation, leaf, nextLeaf);
            }
            append(mutation, nextLeaf, leafLevel);
        } else {
            leaf.add(mutation.getObject());
            structure.getStage().dirty(mutation.getStash(), leaf);
        }
    }
}