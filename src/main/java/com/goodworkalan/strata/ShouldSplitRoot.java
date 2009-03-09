package com.goodworkalan.strata;

/**
 * Determine if contents of the root inner tier should be copied into two inner
 * tier children and then added as the two nodes of the root inner tier.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class ShouldSplitRoot<T, A>
implements RootDecision<T, A>
{
    /**
     * If the root inner tier is currently at the root inner tier capacity then
     * the decision returns true.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     * @return True if root inner tier is at capacity.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> rootLevel, InnerTier<T, A> root)
    {
        return mutation.getStructure().getInnerSize() == root.size();
    }

    /**
     * Add the split root operation to the per level mutation state container.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     */
    public void operation(Mutation<T, A> mutation, Level<T, A> rootLevel, InnerTier<T, A> root)
    {
        rootLevel.operations.add(new SplitRoot<T, A>(root));
    }
}