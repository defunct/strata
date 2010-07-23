package com.goodworkalan.strata;

/**
 * Splits an inner tier.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class SplitInner<T, A>
implements Operation<T, A> {
    /** The parent inner tier of the inner tier to split. */
    private final Tier<T, A> parent;

    /** The inner tier to split. */
    private final Tier<T, A> child;

    /**
     * Create an inner tier split operation.
     * 
     * @param parent
     *            The parent inner tier of the inner tier to split.
     * @param child
     *            The inner tier to split.
     */
    public SplitInner(Tier<T, A> parent, Tier<T, A> child) {
        this.parent = parent;
        this.child = child;
    }

    /**
     * Perform the split.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<T, A> mutation) {
        // Create a new right inner tier.
        Tier<T, A> right = mutation.newInnerTier(child.isChildLeaf());

        // Get the split index.
        int partition = child.getSize() / 2;

        // Copy the contents of the inner tier at and after the split index to
        // the new right inner tier.
        for (int i = partition, stop = child.getSize(); i < stop; i++) {
            right.addBranch(right.getSize(), child.getRecord(i), child.getChildAddress(i));
        }
        child.clear(partition, child.getSize() - partition);

        // The pivot of the parent inner tier of the branch to this inner tier
        // is the pivot copied to the new right inner tier. The pivot in the
        // parent inner tier of the left-most branch in the new right inner tier
        // is null.

        T pivot = right.getRecord(0);
        right.setRecord(0, null);

        int index = parent.getIndexOfChildAddress(child.getAddress());
        parent.addBranch(index + 1, pivot, right.getAddress());

        // Stage the dirty tiers for write.
        Stage<T, A> stage = mutation.getStructure().getStage();
        stage.dirty(mutation.getStash(), parent);
        stage.dirty(mutation.getStash(), child);
        stage.dirty(mutation.getStash(), right);
    }

    /**
     * Return true indicating that this is a split operation.
     * 
     * @return True indicating that this is a split operation.
     */
    public boolean isSplitOrMerge() {
        return true;
    }
}
