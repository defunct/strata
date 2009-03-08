package com.goodworkalan.strata;

import java.util.List;

public final class MergeInner<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> parent;

    // TODO Document.
    private final List<InnerTier<T, A>> listToMerge;

    // TODO Document.
    public MergeInner(InnerTier<T, A> parent, List<InnerTier<T, A>> listToMerge)
    {
        this.parent = parent;
        this.listToMerge = listToMerge;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        InnerTier<T, A> left = listToMerge.get(0);
        InnerTier<T, A> right = listToMerge.get(1);

        int index = parent.getIndex(right.getAddress());
        Branch<T, A> branch = parent.remove(index);

        right.get(0).setPivot(branch.getPivot());
        while (right.size() != 0)
        {
            left.add(right.remove(0));
        }

        TierWriter<T, A> writer = mutation.getStructure().getTierWriter();
        writer.free(mutation.getStash(), right);
        writer.dirty(mutation.getStash(), parent);
        writer.dirty(mutation.getStash(), left);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}