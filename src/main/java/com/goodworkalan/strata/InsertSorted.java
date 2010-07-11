package com.goodworkalan.strata;

import java.util.ListIterator;

/**
 * An insert operation to insert an object value into a leaf. This insert will
 * start from the parent inner tier of the destination child leaf tier. It will
 * find the appropriate child leaf tier among the parent inner tier branches.
 * The child leaf tier must have at least one empty object value positions.
 * <p>
 * This insert operation is for the common case of an insert into a leaf that is
 * not part of a linked list of b+tree leaves of duplicate index values
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class InsertSorted<T, A>
implements LeafOperation<T, A> {
    /**
     * The parent inner tier of the child leaf tier where the object value is to
     * be inserted.
     */
    private final InnerTier<T, A> inner;

    /**
     * Create an insert sorted operation.
     * 
     * @param inner
     *            The parent inner tier of the child leaf tier where the object
     *            value is to be inserted.
     */
    public InsertSorted(InnerTier<T, A> inner) {
        this.inner = inner;
    }

    /**
     * Insert the object value into the appropriate child leaf tier of the
     * parent leaf tier property.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leafLevel
     *            The per level mutation state for the leaf level.
     * @return True of the operation succeeded.
     */
    public boolean operate(Mutation<T, A> mutation, Level<T, A> leafLevel) {
        // Get the collection of the core services of the b+tree.
        Structure<T, A> structure = mutation.getStructure();

        // Find the branch that navigates to the leaf child.
        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        // Insert the object value sorted.
        ListIterator<T> objects = leaf.listIterator();
        while (objects.hasNext()) {
            T before = objects.next();
            if (mutation.getComparable().compareTo(before) <= 0) {
                objects.previous();
                objects.add(mutation.getObject());
                break;
            }
        }

        // If we got to the end, then we need to append the object value.
        if (!objects.hasNext()) {
            objects.add(mutation.getObject());
        }

        // Stage the dirty leaf for write.
        structure.getStage().dirty(mutation.getStash(), leaf);

        // Success.
        return true;
    }
}
