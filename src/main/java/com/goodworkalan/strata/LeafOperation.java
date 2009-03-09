package com.goodworkalan.strata;

/**
 * Perform a mutation on leaf tiers.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
interface LeafOperation<T, A>
{
    /**
     * Perform a mutation on leaf tiers. Returns true if the operation succeeded
     * indicating that the generalized mutation is a success. Returns false if
     * the operation failed indicating that the generalized mutation should be
     * retried.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leafLevel
     *            The per level mutation state for the leaf level.
     * @return True of the operation succeeded.
     */
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf);
}