package com.goodworkalan.strata;

// TODO Document.
final class SplitLinkedListRight<T, A>
implements LeafOperation<T, A>
{
    /** The inner tier parent that references the leaf to split. */
    private final InnerTier<T, A> inner;

    /**
     * Create a split linked list right operaiton.
     * 
     * @param inner
     *            The inner tier parent that references the leaf to split.
     */
    public SplitLinkedListRight(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    private boolean endOfList(Mutation<T, A> mutation, LeafTier<T, A> last)
    {
        return mutation.getStructure().getAllocator().isNull(last.getNext()) || mutation.getStructure().getComparableFactory().newComparable(mutation.getStash(), last.getNext(mutation).get(0)).compareTo(last.get(0)) != 0;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();

        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        LeafTier<T, A> last = leaf;
        while (!endOfList(mutation, last))
        {
            last = last.getNextAndLock(mutation, levelOfLeaf);
        }

        LeafTier<T, A> right = mutation.newLeafTier();
        last.link(mutation, right);

        inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<T, A>(mutation.getObject(), right.getAddress()));

        Allocator<T, A> writer = structure.getAllocator();
        writer.dirty(mutation.getStash(), inner);
        writer.dirty(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), right);

        return new InsertSorted<T, A>(inner).operate(mutation, levelOfLeaf);
    }
}