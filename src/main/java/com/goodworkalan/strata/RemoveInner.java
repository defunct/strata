package com.goodworkalan.strata;

public final class RemoveInner<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> parent;

    // TODO Document.
    private final InnerTier<T, A> child;

    // TODO Document.
    public RemoveInner(InnerTier<T, A> parent, InnerTier<T, A> child)
    {
        this.parent = parent;
        this.child = child;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        int index = parent.getIndex(child.getAddress());

        parent.remove(index);
        if (parent.size() != 0)
        {
            parent.get(0).setPivot(null);
        }

        Stage<T, A> writer = mutation.getStructure().getTierWriter();
        writer.free(mutation.getStash(), child);
        writer.dirty(mutation.getStash(), parent);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}