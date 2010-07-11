package com.goodworkalan.strata;

// TODO Document.
final class ShouldSwapKey<T, A>
implements Decision<T, A> {
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, InnerTier<T, A> parent) {
        Branch<T, A> branch = parent.find(mutation.getComparable());
        if (branch.getPivot() != null && mutation.getComparable().compareTo(branch.getPivot()) == 0) {
            levelOfParent.operations.add(new SwapKey<T, A>(parent));
            return true;
        }
        return false;
    }
}