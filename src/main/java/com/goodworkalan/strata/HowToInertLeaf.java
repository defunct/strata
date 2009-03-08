package com.goodworkalan.strata;


// TODO Document.
final class HowToInertLeaf<T, A>
implements Decision<T, A>
{
    // TODO Document.
    public boolean test(Mutation<T, A> mutation, Level<T, A> levelOfParent, Level<T, A> levelOfChild, InnerTier<T, A> parent)
    {
        Structure<T, A> structure = mutation.getStructure();
        boolean split = true;
        levelOfChild.getSync = new WriteLockExtractor();
        Branch<T, A> branch = parent.find(mutation.getComparable());
        LeafTier<T, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());
        levelOfChild.getSync = new WriteLockExtractor();
        levelOfChild.lockAndAdd(leaf);
        if (leaf.size() == structure.getLeafSize())
        {
            Comparable<? super T> first = mutation.getStructure().getComparableFactory().newComparable(mutation.getStash(), leaf.get(0));
            if (first.compareTo(leaf.get(leaf.size() - 1)) == 0)
            {
                int compare = mutation.getComparable().compareTo(leaf.get(0));
                if (compare < 0)
                {
                    mutation.leafOperation = new SplitLinkedListLeft<T, A>(parent);
                }
                else if (compare > 0)
                {
                    mutation.leafOperation = new SplitLinkedListRight<T, A>(parent);
                }
                else
                {
                    mutation.leafOperation = new InsertLinkedList<T, A>(leaf);
                    split = false;
                }
            }
            else
            {
                levelOfParent.listOfOperations.add(new SplitLeaf<T, A>(parent));
                mutation.leafOperation = new InsertSorted<T, A>(parent);
            }
        }
        else
        {
            mutation.leafOperation = new InsertSorted<T, A>(parent);
            split = false;
        }
        return split;
    }
}