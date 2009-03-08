package com.goodworkalan.strata;

import java.util.ListIterator;

final class InsertSorted<T, A>
implements LeafOperation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> inner;

    // TODO Document.
    public InsertSorted(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    public boolean operate(Mutation<T, A> mutation, Level<T, A> levelOfLeaf)
    {
        Structure<T, A> structure = mutation.getStructure();

        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        ListIterator<T> objects = leaf.listIterator();
        while (objects.hasNext())
        {
            T before = objects.next();
            if (mutation.getComparable().compareTo(before) <= 0)
            {
                objects.previous();
                objects.add(mutation.getObject());
                break;
            }
        }

        if (!objects.hasNext())
        {
            objects.add(mutation.getObject());
        }

        // FIXME Now we are writing before we are splitting. Problem.
        // Empty cache does not work!
        structure.getTierWriter().dirty(mutation.getStash(), leaf);

        return true;
    }
}