package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.link;

// TODO Document.
final class SplitLeaf<T, A>
implements Operation<T, A>
{
    // TODO Document.
    private final InnerTier<T, A> inner;

    // TODO Document.
    public SplitLeaf(InnerTier<T, A> inner)
    {
        this.inner = inner;
    }

    // TODO Document.
    public void operate(Mutation<T, A> mutation)
    {
        Structure<T, A> structure = mutation.getStructure();

        Branch<T, A> branch = inner.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

        int middle = leaf.size() >> 1;
        boolean odd = (leaf.size() & 1) == 1;
        int lesser = middle - 1;
        int greater = odd ? middle + 1 : middle;

        int partition = -1;
        Comparable<? super T> candidate = mutation.getStructure().getComparableFactory().newComparable(mutation.getStash(), leaf.get(middle));
        for (int i = 0; partition == -1 && i < middle; i++)
        {
            if (candidate.compareTo(leaf.get(lesser)) != 0)
            {
                partition = lesser + 1;
            }
            else if (candidate.compareTo(leaf.get(greater)) != 0)
            {
                partition = greater;
            }
            lesser--;
            greater++;
        }

        LeafTier<T, A> right = mutation.newLeafTier();

        while (partition != leaf.size())
        {
            right.add(leaf.remove(partition));
        }

        link(mutation, leaf, right);

        int index = inner.getIndex(leaf.getAddress());
        inner.add(index + 1, new Branch<T, A>(right.get(0), right.getAddress()));

        Stage<T, A> writer = structure.getStage();
        writer.dirty(mutation.getStash(), inner);
        writer.dirty(mutation.getStash(), leaf);
        writer.dirty(mutation.getStash(), right);
    }

    /**
     * Return true indicating that this is a merge operation.
     * 
     * @return True indicating that this is a merge operation.
     */
    public boolean isSplitOrMerge()
    {
        return true;
    }
}