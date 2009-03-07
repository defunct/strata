package com.goodworkalan.strata;

import java.util.ListIterator;

// TODO Document.
final class LeafInsert<B, A>
implements Decision<B, A>
{
    // TODO Document.
    public boolean test(Mutation<B, A> mutation, Level<B, A> levelOfParent, Level<B, A> levelOfChild, InnerTier<B, A> parent)
    {
        Structure<B, A> structure = mutation.getStructure();
        boolean split = true;
        levelOfChild.getSync = new WriteLockExtractor();
        Branch<B, A> branch = parent.find(mutation.getComparable());
        LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());
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
                    mutation.leafOperation = new LeafInsert.SplitLinkedListLeft<B, A>(parent);
                }
                else if (compare > 0)
                {
                    mutation.leafOperation = new LeafInsert.SplitLinkedListRight<B, A>(parent);
                }
                else
                {
                    mutation.leafOperation = new LeafInsert.InsertLinkedList<B, A>(leaf);
                    split = false;
                }
            }
            else
            {
                levelOfParent.listOfOperations.add(new LeafInsert.SplitLeaf<B, A>(parent));
                mutation.leafOperation = new LeafInsert.InsertSorted<B, A>(parent);
            }
        }
        else
        {
            mutation.leafOperation = new LeafInsert.InsertSorted<B, A>(parent);
            split = false;
        }
        return split;
    }

    // TODO Document.
    private final static class SplitLinkedListLeft<B, A>
    implements LeafOperation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> inner;

        // TODO Document.
        public SplitLinkedListLeft(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        // TODO Document.
        public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf)
        {
            Structure<B, A> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

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

            TierWriter<B, A> writer = structure.getWriter();
            writer.dirty(mutation.getStash(), inner);
            writer.dirty(mutation.getStash(), leaf);
            writer.dirty(mutation.getStash(), right);

            return new LeafInsert.InsertSorted<B, A>(inner).operate(mutation, levelOfLeaf);
        }
    }

    // TODO Document.
    private final static class SplitLinkedListRight<B, A>
    implements LeafOperation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> inner;

        // TODO Document.
        public SplitLinkedListRight(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        // TODO Document.
        private boolean endOfList(Mutation<B, A> mutation, LeafTier<B, A> last)
        {
            return mutation.getStructure().getAllocator().isNull(last.getNext()) || mutation.newComparable(last.getNext(mutation).get(0)).compareTo(last.get(0)) != 0;
        }

        // TODO Document.
        public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf)
        {
            Structure<B, A> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

            LeafTier<B, A> last = leaf;
            while (!endOfList(mutation, last))
            {
                last = last.getNextAndLock(mutation, levelOfLeaf);
            }

            LeafTier<B, A> right = mutation.newLeafTier();
            last.link(mutation, right);

            inner.add(inner.getIndex(leaf.getAddress()) + 1, new Branch<B, A>(mutation.bucket, right.getAddress()));

            TierWriter<B, A> writer = structure.getWriter();
            writer.dirty(mutation.getStash(), inner);
            writer.dirty(mutation.getStash(), leaf);
            writer.dirty(mutation.getStash(), right);

            return new LeafInsert.InsertSorted<B, A>(inner).operate(mutation, levelOfLeaf);
        }
    }

    // TODO Document.
    private final static class SplitLeaf<B, A>
    implements Operation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> inner;

        // TODO Document.
        public SplitLeaf(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        // TODO Document.
        public void operate(Mutation<B, A> mutation)
        {
            Structure<B, A> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

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

            TierWriter<B, A> writer = structure.getWriter();
            writer.dirty(mutation.getStash(), inner);
            writer.dirty(mutation.getStash(), leaf);
            writer.dirty(mutation.getStash(), right);
        }

        // TODO Document.
        public boolean canCancel()
        {
            return true;
        }
    }

    // TODO Document.
    private final static class InsertSorted<B, A>
    implements LeafOperation<B, A>
    {
        // TODO Document.
        private final InnerTier<B, A> inner;

        // TODO Document.
        public InsertSorted(InnerTier<B, A> inner)
        {
            this.inner = inner;
        }

        // TODO Document.
        public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf)
        {
            Structure<B, A> structure = mutation.getStructure();

            Branch<B, A> branch = inner.find(mutation.getComparable());
            LeafTier<B, A> leaf = structure.getPool().getLeafTier(mutation.getStash(), branch.getAddress());

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
            structure.getWriter().dirty(mutation.getStash(), leaf);

            return true;
        }
    }

    // TODO Document.
    private final static class InsertLinkedList<B, A>
    implements LeafOperation<B, A>
    {
        // TODO Document.
        private final LeafTier<B, A> leaf;

        // TODO Document.
        public InsertLinkedList(LeafTier<B, A> leaf)
        {
            this.leaf = leaf;
        }

        // TODO Document.
        public boolean operate(Mutation<B, A> mutation, Level<B, A> levelOfLeaf)
        {
            leaf.append(mutation, levelOfLeaf);
            return true;
        }
    }
}