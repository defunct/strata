package com.goodworkalan.strata;

// TODO Document.
public final class ShouldSplitRoot<T, A>
implements RootDecision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation,
                        Level<T, A> levelOfRoot,
                        InnerTier<T, A> root)
    {
        return mutation.getStructure().getInnerSize() == root.size();
    }

    // TODO Document.
    public void operation(Mutation<T, A> mutation,
                          Level<T, A> levelOfRoot,
                          InnerTier<T, A> root)
    {
        levelOfRoot.listOfOperations.add(new SplitRoot<T, A>(root));
    }
}