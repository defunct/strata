package com.goodworkalan.strata;


/**
 * Merge an inner tier by 
 * @author alan
 *
 * @param <Record>
 *            The value type of the b+tree objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public final class MergeInner<Record, Address>
implements Operation<Record, Address> {
    /** The parent teir. */
    private final Tier<Record, Address> parent;

    /** The left child inner tier. */
    private final Tier<Record, Address> left;
    
    /** The right child inner tier to merge into the left child inner tier. */
    private final Tier<Record, Address> right;

    /**
     * Merge the right child inner tier into the left child inner tier.
     *  
     * @param parent
     *            The parent tier.
     * @param left
     *            The left child inner tier.
     * @param right
     *            The right child inner tier to merge into the left child inner
     *            tier.
     */
    public MergeInner(Tier<Record, Address> parent, Tier<Record, Address> left, Tier<Record, Address> right) {
        this.parent = parent;
        this.left = left;
        this.right = right;
    }

    /**
     * Merge the left 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<Record, Address> mutation) {
        int index = parent.getIndexOfChildAddress(right.getAddress());
        Record pivot = parent.getRecord(index);
        parent.clear(index, 1);

        right.setRecord(0, pivot);
        for (int i = 0, stop = right.getSize(); i < stop; i++) {
            left.addBranch(i, right.getRecord(i), right.getChildAddress(i));
        }

        Stage<Record, Address> writer = mutation.getStructure().getStage();
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