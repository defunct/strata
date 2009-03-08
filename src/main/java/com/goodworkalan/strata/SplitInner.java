package com.goodworkalan.strata;

public final class SplitInner<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> parent;

    // TODO Document.
    private final InnerTier<T, A> child;

    // TODO Document.
    public SplitInner(InnerTier<T, A> parent, InnerTier<T, A> child)
    {
        this.parent = parent;
        this.child = child;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        InnerTier<T, A> right = mutation.newInnerTier(child.getChildType());

        int partition = child.size() / 2;

        while (partition < child.size())
        {
            right.add(child.remove(partition));
        }

        T pivot = right.get(0).getPivot();
        right.get(0).setPivot(null);

        int index = parent.getIndex(child.getAddress());
        parent.add(index + 1, new Branch<T, A>(pivot, right.getAddress()));

        Stage<T, A> allocator = mutation.getStructure().getStage();
        allocator.dirty(mutation.getStash(), parent);
        allocator.dirty(mutation.getStash(), child);
        allocator.dirty(mutation.getStash(), right);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}