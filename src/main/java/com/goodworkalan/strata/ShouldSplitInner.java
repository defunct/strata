package com.goodworkalan.strata;

// TODO Document.
final class ShouldSplitInner<T, A>
implements Decision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, InnerTier<T, A> parent)
    {
        Structure<T, A> structure = mutation.getStructure();
        Branch<T, A> branch = parent.find(mutation.getComparable());
        InnerTier<T, A> child = structure.getPool().getInnerTier(mutation.getStash(), branch.getAddress());
        levelOfChild.lockAndAdd(child);
        if (child.size() == structure.getInnerSize())
        {
            levelOfParent.listOfOperations.add(new SplitInner<T, A>(parent, child));
            return true;
        }
        return false;
    }
}