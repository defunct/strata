package com.goodworkalan.strata;

/**
 * An inner tier operation that copies the contents of a single remaining
 * child inner tier into the root. The single remaining child tier will
 * be the product of a merge operation.
 *  
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class FillRoot<T, A>
implements Operation<T, A> {
    /** The root inner tier. */
    private final InnerTier<T, A> root;

    /**
     * Create a fill root operation with the given root inner tier.
     * 
     * @param root
     *            The root inner tier.
     */
    public FillRoot(InnerTier<T, A> root) {
        this.root = root;
    }

    /**
     * Merge the two child tiers by copying them into the root tier.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<T, A> mutation) {
        // FIXME Size is off.
        if (root.size() != 0) {
            throw new IllegalStateException();
        }

        Structure<T, A> structure = mutation.getStructure();

        InnerTier<T, A> child = structure.getPool().getInnerTier(mutation.getStash(), root.remove(0).getAddress());
        while (child.size() != 0) {
            root.add(child.remove(0));
        }

        root.setChildType(child.getChildType());

        Stage<T, A> allocator = structure.getStage();
        allocator.free(mutation.getStash(), child);
        allocator.dirty(mutation.getStash(), root);
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
