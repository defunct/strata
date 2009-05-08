package com.goodworkalan.strata;

/**
 * An inner tier decision that never adds operations and always returns false.
 * 
 * @author Alan Gutierrez
 *
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class InnerNever<T, A>
implements Decision<T, A>
{
    /**
     * Add no operations and return false.
     * 
     * @param mutation
     *            The mutation state container.
     * @param parentLevel
     *            The operations to perform on the parent tier.
     * @param childLevel
     *            The operations to perform on the child tier.
     * @param parent
     *            The parent tier.
     * @return False indicating that there are no operations at that no
     *         exclusive locks are required.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> parentLevel, Level<T, A> childLevel, InnerTier<T, A> parent)
    {
        return false;
    }
}