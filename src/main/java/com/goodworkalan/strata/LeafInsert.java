package com.goodworkalan.strata;

import java.util.ListIterator;

final class LeafInsert<B, A, X>
implements Decision<B, A, X>
{
    public boolean test(Mutation<B, A, X> mutation, Level<B, A, X> levelOfParent, Level<B, A, X> levelOfChild, InnerTier<B, A> parent)
    {
        Structure<B, A, X> structure = mutation.getStructure();
        boolean split = true;
        levelOfChild.getSync = new WriteLockExtractor();
        Branch<B, A> branch = parent.find(mutation.getComparable());
        LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), branch.getAddress());
        levelOfChild.getSync = new WriteLockExtractor();
        levelOfChild.lockAndAdd(leaf);
        if (leaf.size() == structure.getLeafSize())
        {
            Comparable<B> first = mutation.newComparable(leaf.get(0));
            if (first.compareTo(leaf.get(leaf.size() - 1)) == 0)
            {
                int compare = mutation.getComparable().compareTo(leaf.get(0));
                if (compare < 0)
                {
                    mutation.leafOperation = new LeafInsert.SplitLinkedListLeft<B, A, X>(parent);
                }
                else if (compare > 0)
                {
                    mutation.leafOperation = new LeafInsert.SplitLinkedListRight<B, A, X>(parent);
                }
                else
                {
                    mutation.leafOperation = new LeafInsert.InsertLinkedList<B, A, X>(leaf);
                    split = false;
                }
            }
            else
            {
                levelOfParent.listOfOperations.add(new LeafInsert.SplitLeaf<B, A, X>(parent));
                mutation.leafOperation = new LeafInsert.InsertSorted<B, A, X>(parent);
            }
        }
        else
        {
            mutation.leafOperation = new LeafInsert.InsertSorted<B, A, X>(parent);
            split = false;
        }
        return split;
    }

    private final static class SplitLinkedListLeft<B, A, X>
    implements LeafOperation<B, A, X>
    {
        private final InnerTier<B, A> inner;

        public SplitLinkedListLeft(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            Structure<B, A, X> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), branch.getAddress());

            LeafTier<B, A> right = mutation.newLeafTier();
            while (leaf.size() != 0)
            {
                right.add(leaf.remove(0));
            }

            leaf.link(mutation, right);

            int index = inner.getIndex(leaf.getAddress());
            if (index != 0)
            {
                throw new IllegalStateException();
            }
            inner.add(index + 1, new Branch<B, A>(right.get(0), right.getAddress()));

            TierWriter<B, A, X> writer = structure.getWriter();
            writer.dirty(mutation.getTxn(), inner);
            writer.dirty(mutation.getTxn(), leaf);
            writer.dirty(mutation.getTxn(), right);

            return new LeafInsert.InsertSorted<B, A, X>(inner).operate(mutation, levelOfLeaf);
        }
    }

    private final static class SplitLinkedListRight<B, A, X>
    implements LeafOperation<B, A, X>
    {
        private final InnerTier<B, A> inner;

        public SplitLinkedListRight(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        private boolean endOfList(Mutation<B, A, X> mutation, LeafTier<B, A> last)
        {
            return mutation.getStructure().getAllocator().isNull(last.getNext()) || mutation.newComparable(last.getNext(mutation).get(0)).compareTo(last.get(0)) != 0;
        }

        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            Structure<B, A, X> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), branch.getAddress());

            LeafTier<B, A> last = leaf;
            while (!endOfList(mutation, last))
            {
                last = last.getNextAndLock(mutation, levelOfLeaf);
            }

            LeafTier<B, A> right = mutation.newLeafTier();
            last.link(mutation, right);

            inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<B, A>(mutation.bucket, right.getAddress()));

            TierWriter<B, A, X> writer = structure.getWriter();
            writer.dirty(mutation.getTxn(), inner);
            writer.dirty(mutation.getTxn(), leaf);
            writer.dirty(mutation.getTxn(), right);

            return new LeafInsert.InsertSorted<B, A, X>(inner).operate(mutation, levelOfLeaf);
        }
    }

    private final static class SplitLeaf<B, A, X>
    implements Operation<B, A, X>
    {
        private final InnerTier<B, A> inner;

        public SplitLeaf(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        public void operate(Mutation<B, A, X> mutation)
        {
            Structure<B, A, X> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), branch.getAddress());

            int middle = leaf.size() >> 1;
            boolean odd = (leaf.size() & 1) == 1;
            int lesser = middle - 1;
            int greater = odd ? middle + 1 : middle;

            int partition = -1;
            Comparable<B> candidate = mutation.newComparable(leaf.get(middle));
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

            LeafTier<B, A> right = mutation.newLeafTier();

            while (partition != leaf.size())
            {
                right.add(leaf.remove(partition));
            }

            leaf.link(mutation, right);

            int index = inner.getIndex(leaf.getAddress());
            inner.add(index + 1, new Branch<B, A>(right.get(0), right.getAddress()));

            TierWriter<B, A, X> writer = structure.getWriter();
            writer.dirty(mutation.getTxn(), inner);
            writer.dirty(mutation.getTxn(), leaf);
            writer.dirty(mutation.getTxn(), right);
        }

        public boolean canCancel()
        {
            return true;
        }
    }

    private final static class InsertSorted<B, A, X>
    implements LeafOperation<B, A, X>
    {
        private final InnerTier<B, A> inner;

        public InsertSorted(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            Structure<B, A, X> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getTxn(), branch.getAddress());

            ListIterator<B> objects = leaf.listIterator();
            while (objects.hasNext())
            {
                B before = objects.next();
                if (mutation.getComparable().compareTo(before) <= 0)
                {
                    objects.previous();
                    objects.add(mutation.bucket);
                    break;
                }
            }

            if (!objects.hasNext())
            {
                objects.add(mutation.bucket);
            }

            // FIXME Now we are writing before we are splitting. Problem.
            // Empty cache does not work!
            structure.getWriter().dirty(mutation.getTxn(), leaf);

            return true;
        }
    }

    private final static class InsertLinkedList<B, A, X>
    implements LeafOperation<B, A, X>
    {
        private final LeafTier<B, A> leaf;

        public InsertLinkedList(LeafTier<B, A> leaf)
        {
            this.leaf = leaf;
        }

        public boolean operate(Mutation<B, A, X> mutation, Level<B, A, X> levelOfLeaf)
        {
            leaf.append(mutation, levelOfLeaf);
            return true;
        }
    }
}