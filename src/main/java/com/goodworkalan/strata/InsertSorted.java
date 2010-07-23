package com.goodworkalan.strata;


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
    private final Tier<T, A> inner;

    /**
     * Create an insert sorted operation.
     * 
     * @param inner
     *            The parent inner tier of the child leaf tier where the object
     *            value is to be inserted.
     */
    public InsertSorted(Tier<T, A> inner) {
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
        int branch = inner.find(mutation.getComparable());
        Tier<T, A> leaf = structure.getPool().get(mutation.getStash(), inner.getChildAddress(branch));

        int i = 0, stop = leaf.getSize();
        // Insert the object value sorted.
        for (; i < stop; i++) {
            T before = leaf.getRecord(i);
            if (mutation.getComparable().compareTo(before) <= 0) {
                leaf.addRecord(i, mutation.getObject());
                break;
            }
        }

        // If we got to the end, then we need to append the object value.
        if (i == stop) {
            leaf.addRecord(stop, mutation.getObject());
        }

        // Stage the dirty leaf for write.
        structure.getStage().dirty(mutation.getStash(), leaf);

        // Success.
        return true;
    }
}
