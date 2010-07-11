package com.goodworkalan.strata;

/**
 * Makes a decision to lock the root inner tier exclusively and adds the
 * operations necessary to perform a mutative operation on the parent or child
 * tiers to the parent level and child levels.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
interface RootDecision<T, A> {
    /**
     * Determine if the operations performed by this root decision are
     * applicable for the given root inner tier.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     * @return True if the operations performed by this root decision are
     *         applicable.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> rootLevel, InnerTier<T, A> root);

    /**
     * Add the operations for this root decision to the per level mutation state
     * container for the given root inner tier.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     */
    public void operation(Mutation<T, A> mutation, Level<T, A> rootLevel, InnerTier<T, A> root);
}
