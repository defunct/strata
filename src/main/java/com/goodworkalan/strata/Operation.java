package com.goodworkalan.strata;

/**
 * An operation that mutates tiers during a generalized mutation.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
interface Operation<T, A>
{
    /**
     * Perform the operation.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<T, A> mutation);

    /**
     * Return true if the operation is a split or merge operation.
     * 
     * @return True if the operation is a split or merge operation.
     */
    public boolean isSplitOrMerge();
}