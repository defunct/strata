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
    private final InnerTier<T, A> parent;

    /** The inner tier to split. */
    private final InnerTier<T, A> child;

    /**
     * Create an inner tier split operation.
     * 
     * @param parent
     *            The parent inner tier of the inner tier to split.
     * @param child
     *            The inner tier to split.
     */
    public SplitInner(InnerTier<T, A> parent, InnerTier<T, A> child) {
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
        InnerTier<T, A> right = mutation.newInnerTier(child.getChildType());

        // Get the split index.
        int partition = child.size() / 2;

        // Copy the contents of the inner tier at and after the split index to
        // the new right inner tier.
        while (partition < child.size()) {
            right.add(child.remove(partition));
        }

        // The pivot of the parent inner tier of the branch to this inner tier
        // is the pivot copied to the new right inner tier. The pivot in the
        // parent inner tier of the left-most branch in the new right inner tier
        // is null.

        T pivot = right.get(0).getPivot();
        right.get(0).setPivot(null);

        int index = parent.getIndex(child.getAddress());
        parent.add(index + 1, new Branch<T, A>(pivot, right.getAddress()));

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
