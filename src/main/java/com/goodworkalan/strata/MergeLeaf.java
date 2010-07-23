package com.goodworkalan.strata;

// TODO Document.
public final class MergeLeaf<T, A>
implements Operation<T, A> {
    /** The parent inner tier. */
    private final Tier<T, A> parent;

    /** The left leaf tier. */
    private final Tier<T, A> left;

    /** The right leaf tier. */
    private final Tier<T, A> right;

    /**
     * Create a new merge leaf operation.
     * 
     * @param parent
     *            The parent inner tier.
     * @param left
     *            The left leaf tier.
     * @param right
     *            The right leaf tier.
     */
    public MergeLeaf(Tier<T, A> parent, Tier<T, A> left, Tier<T, A> right) {
        this.parent = parent;
        this.left = left;
        this.right = right;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation) {
        parent.clear(parent.getIndexOfChildAddress(right.getAddress()), 1);

        for (int i = 0, stop = right.getSize(); i < stop; i++) {
            left.addRecord(left.getSize(), right.getRecord(i));
        }
        
        // FIXME Get last leaf. 
        left.setNext(right.getNext());

        Stage<T, A> writer = mutation.getStructure().getStage();
        writer.free(mutation.getStash(), right);
        writer.dirty(mutation.getStash(), parent);
        writer.dirty(mutation.getStash(), left);
    }

    /**
     * Return true indicating that this is a merge operation.
     * 
     * @return True indicating that this is a merge operation.
     */
    public boolean isSplitOrMerge() {
        return true;
    }
}