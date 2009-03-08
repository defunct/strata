package com.goodworkalan.strata;

final class SplitRoot<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> root;

    // TODO Document.
    public SplitRoot(InnerTier<T, A> root)
    {
        this.root = root;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        InnerTier<T, A> left = mutation.newInnerTier(root.getChildType());
        InnerTier<T, A> right = mutation.newInnerTier(root.getChildType());
        
        int partition = root.size() / 2;
        int fullSize = root.size();
        for (int i = 0; i < partition; i++)
        {
            left.add(root.remove(0));
        }
        for (int i = partition; i < fullSize; i++)
        {
            right.add(root.remove(0));
        }
        T pivot = right.get(0).getPivot();
        right.get(0).setPivot(null);

        root.add(new Branch<T, A>(null, left.getAddress()));
        root.add(new Branch<T, A>(pivot, right.getAddress()));

        root.setChildType(ChildType.INNER);

        Stage<T, A> writer = mutation.getStructure().getTierWriter();
        writer.dirty(mutation.getStash(), root);
        writer.dirty(mutation.getStash(), left);
        writer.dirty(mutation.getStash(), right);
    }

    // TODO Document.
    public boolean canCancel()
    {
        return true;
    }
}