package com.goodworkalan.strata;

// TODO Document.
final class ShouldSwapKey<B, A>
implements Decision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation,
                        Level<B, A> levelOfParent,
                        Level<B, A> levelOfChild,
                        InnerTier<B, A> parent)
    {
        Branch<B, A> branch = parent.find(mutation.getComparable());
        if (branch.getPivot() != null && mutation.getComparable().compareTo(branch.getPivot()) == 0)
        {
            levelOfParent.listOfOperations.add(new Swap<B, A>(parent));
            return true;
        }
        return false;
    }
}