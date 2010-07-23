package com.goodworkalan.strata;

/**
 * An inner tier operation that copies the contents of a single remaining
 * child inner tier into the root. The single remaining child tier will
 * be the product of a merge operation.
 *  
 * @author Alan Gutierrez
 *
 * @param <Record>
 *            The value type of the b+tree objects.
 * @param <Address>
 *            The address type used to identify an inner or leaf tier.
 */
public final class FillRoot<Record, Address>
implements Operation<Record, Address> {
    /** The root inner tier. */
    private final Tier<Record, Address> root;

    /**
     * Create a fill root operation with the given root inner tier.
     * 
     * @param root
     *            The root inner tier.
     */
    public FillRoot(Tier<Record, Address> root) {
        this.root = root;
    }

    /**
     * Merge the two child tiers by copying them into the root tier.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<Record, Address> mutation) {
        // FIXME Size is off.
        if (root.getSize() != 0) {
            throw new IllegalStateException();
        }

        Structure<Record, Address> structure = mutation.getStructure();

        Tier<Record, Address> child = structure.getPool().get(mutation.getStash(), root.getChildAddress(0));
        root.clear(0, 1);
        for (int i = 0, stop = child.getSize(); i < stop; i++) {
            root.addBranch(root.getSize(), child.getRecord(i), child.getChildAddress(i));
        }
        child.clear(0, child.getSize());

        root.setChildLeaf(child.isChildLeaf());

        Stage<Record, Address> stage = structure.getStage();
        stage.free(mutation.getStash(), child);
        stage.dirty(mutation.getStash(), root);
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
