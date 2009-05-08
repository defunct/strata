package com.goodworkalan.strata;

/**
 * Split a full root tier by copying its contents into two new leaf tiers,
 * emptying the root tier, and then adding two branches to the new leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class SplitRoot<T, A>
implements Operation<T, A>
{
    /** The root tier. */
    private final InnerTier<T, A> root;

    /**
     * Create a split root operation with the given root inner tier.
     * 
     * @param root
     *            The root inner tier.
     */
    public SplitRoot(InnerTier<T, A> root)
    {
        this.root = root;
    }

    /**
     * Perform the split.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<T, A> mutation)
    {
        // Create new left and right inner tiers.
        InnerTier<T, A> left = mutation.newInnerTier(root.getChildType());
        InnerTier<T, A> right = mutation.newInnerTier(root.getChildType());
        
        // Find the partition index and move the branches up to the partition
        // into the left inner tier. Move the branches at and after the partiion
        // into the right inner tier.

        int partition = root.size() / 2;
        int fullSize = root.size();
        for (int i = 0; i < partition; i++)
        {
            left.add(root.remove(0));
        }
        for (int i = partition; i < fullSize; i++)
        {
            right.add(root.remove(0));
        }

        // The left-most pivot or the right inner tier is null.

        T pivot = right.get(0).getPivot();
        right.get(0).setPivot(null);

        // Add the branches to the new left and right inner tiers to the now
        // empty root tier.

        root.add(new Branch<T, A>(null, left.getAddress()));
        root.add(new Branch<T, A>(pivot, right.getAddress()));

        // Set the child type of the root tier to inner.
        root.setChildType(ChildType.INNER);

        // Stage the dirty tiers for write.
        Stage<T, A> stage = mutation.getStructure().getStage();
        stage.dirty(mutation.getStash(), root);
        stage.dirty(mutation.getStash(), left);
        stage.dirty(mutation.getStash(), right);
    }

    /**
     * Return true indicating that this is a split operation.
     * 
     * @return True indicating that this is a split operation.
     */
    public boolean isSplitOrMerge()
    {
        return true;
    }
}
