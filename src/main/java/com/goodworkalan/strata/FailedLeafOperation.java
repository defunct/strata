package com.goodworkalan.strata;

/**
 * A leaf operation that always fails.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
public final class FailedLeafOperation<T, A> implements LeafOperation<T, A> {
    /**
     * Return false to indicate that the leaf operation failed.
     * 
     * @param mutation
     *            The mutation state container.
     * @param leafLevel
     *            The per level mutation state for the leaf level.
     * @return False to indicate failure.
     */
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf) {
        return false;
    }
}