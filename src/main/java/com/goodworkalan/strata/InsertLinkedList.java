package com.goodworkalan.strata;

final class InsertLinkedList<T, A>
implements LeafOperation<T, A>
{
    // TODO Document.
    private final LeafTier<T, A> leaf;

    // TODO Document.
    public InsertLinkedList(LeafTier<T, A> leaf)
    {
        this.leaf = leaf;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        leaf.append(mutation, levelOfLeaf);
        return true;
    }
}