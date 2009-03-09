package com.goodworkalan.strata;

/**
 * Makes a decision to lock the parent inner tier exclusively and adds the
 * operations necessary to perform a mutative operation on the parent or child
 * tiers to the parent and child levels.
 * 
 * @author Alan Gutierrez
 * 
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
interface Decision<T, A>
{
    /**
     * Make a decision to lock the parent tier exclusively and adds the
     * operations necessary to perform a mutative operation on the parent or
     * child tiers to the parent and child levels.
     * 
     * @param mutation
     *            The mutation state container.
     * @param parentLevel
     *            The operations to perform on the parent tier.
     * @param childLevel
     *            The operations to perform on the child tier.
     * @param parent
     *            The parent tier.
     * @return True if this decision added operations to the parent or child
     *         tier and requires that tiers be locked exclusively as the tree is
     *         decended.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> parentLevel, Level<T, A> childLevel, InnerTier<T, A> parent);
}
