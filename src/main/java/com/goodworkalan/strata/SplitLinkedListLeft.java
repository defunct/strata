package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.link;

// TODO Document.
final class SplitLinkedListLeft<T, A>
implements LeafOperation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> inner;

    // TODO Document.
    public SplitLinkedListLeft(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();

        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        LeafTier<T, A> right = mutation.newLeafTier();
        while (leaf.size() != 0)
        {
            right.add(leaf.remove(0));
        }

        link(mutation, leaf, right);

        int index = inner.getIndex(leaf.getAddress());
        if (index != 0)
        {
            throw new IllegalStateException();
        }
        inner.add(index + 1, new Branch<T, A>(right.get(0), right.getAddress()));

        Stage<T, A> allocator = structure.getStage();
        allocator.dirty(mutation.getStash(), inner);
        allocator.dirty(mutation.getStash(), leaf);
        allocator.dirty(mutation.getStash(), right);

        return new InsertSorted<T, A>(inner).operate(mutation, levelOfLeaf);
    }
}