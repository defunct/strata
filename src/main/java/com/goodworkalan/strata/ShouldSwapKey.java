package com.goodworkalan.strata;

/**
 * For remove, decide whether if the object removed is an inner tier pivot and
 * needs to be swapped.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 * @param <A>
 */
final class ShouldSwapKey<T, A>
implements Decision<T, A> {
    /** Construct a should swap key decision. */
    public ShouldSwapKey() {
    }

    /**
     * @param mutation
     *            The tree mutation state.
     * @param levelOfParent
     *            The parent level.
     * @param levelOfChild
     *            The child level.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, Tier<T, A> parent) {
        int branch = parent.find(mutation.getComparable());
        T pivot = parent.getRecord(branch);
        if (pivot != null && mutation.getComparable().compareTo(pivot) == 0) {
            levelOfParent.operations.add(new SwapKey<T, A>(parent));
            return true;
        }
        return false;
    }
}