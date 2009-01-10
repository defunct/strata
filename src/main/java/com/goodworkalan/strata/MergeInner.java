package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.List;

/**
 * A decision that determines whether to merge two inner tiers into one
 * tier or else to delete an inner tier that has only one child tier but
 * is either the only child tier or its siblings are already full.
 * <h3>Only Children</h3>
 * <p>
 * It is possible that an inner tier may have only one child leaf or inner
 * tier. This occurs in the case where the siblings of of inner tier are
 * at capacity. A merge occurs when two children are combined. The nodes
 * from the child to the right are combined with the nodes from the child
 * to the left. The parent branch that referenced the right child is
 * deleted. 
 * <p>
 * If it is the case that a tier is next to full siblings, as leaves are
 * deleted from that tier, it will not be a candidate to merge with a
 * sibling until it reaches a size of one. At that point, it could merge
 * with a sibling if a deletion were to cause its size to reach zero.
 * <p>
 * However, the single child of that tier has no siblings with which it
 * can merge. A tier with a single child cannot reach a size of zero by
 * merging.
 * <p>
 * If were where to drain the subtree of an inner tier with a single child
 * of every leaf, we would merge its leaf tiers and merge its inner tiers
 * until we had subtree that consisted solely of inner tiers with one
 * child and a leaf with one item. At that point, when we delete the last
 * item, we need to delete the chain of tiers with only children.
 * <p>
 * We deleting any child that is size of one that cannot merge with a
 * sibling. Deletion means freeing the child and removing the branch that
 * references it.
 * <p>
 * The only leaf child will not have a sibling with which it can merge,
 * however. We won't be able to copy leaf items from a right leaf to a
 * left leaf. This means we won't be able to update the linked list of
 * leaves, unless we go to the left of the only child. But, going to the
 * left of the only child requires knowing that we must go to the left.
 * <p>
 * We are not going to know which left to take the first time down,
 * though. The actual pivot is not based on the number of children. It
 * might be above the point where the list of only children begins. As
 * always, it is a pivot whose value matches the first item in the
 * leaf, in this case the only item in the leaf.
 * <p>
 * Here's how it works.
 * <p>
 * On the way down, we look for a branch that has an inner tier that is
 * size of one. If so, we set a flag in the mutator to note that we are
 * now deleting.
 * <p>
 * If we encounter an inner tier has more than one child on the way down we
 * are not longer in the deleting state.
 * <p>
 * When we reach the leaf, if it has a size of one and we are in the
 * deleting state, then we look in the mutator for a left leaf variable
 * and an is left most flag. More on those later as neither are set.
 * <p>
 * We tell the mutator that we have a last item and that the action has
 * failed, by setting the fail action. Failure means we try again.
 * <p>
 * On the retry, as we descend the tree, we have the last item variable
 * set in the mutator. 
 * <p>
 * Note that we are descending the tree again. Because we are a concurrent
 * data structure, the structure of the tree may change. I'll get to that.
 * For now, let's assume that it has not changed.
 * <p>
 * If it has not changed, then we are going to encounter a pivot that has
 * our last item. When we encounter this pivot, we are going to go left.
 * Going left means that we descend to the child of the branch before the
 * branch of the pivot. We then follow each rightmost branch of each inner
 * tier until we reach the right most leaf. That leaf is the leaf before
 * the leaf that is about to be removed. We store this in the mutator.
 * <p>
 * Of course, the leaf to be removed may be the left most leaf in the
 * entire data structure. In that case, we set a variable named left most
 * in the mutator.
 * <p>
 * When we go left, we lock every inner tier and the leaf tier exclusive,
 * to prevent it from being changed by another query in another thread.
 * We always lock from left to right.
 * <p>
 * Now we continue our descent. Eventually, we reach out chain of inner
 * tiers with only one child. That chain may only be one level deep, but
 * there will be such a chain.
 * <p>
 * Now we can add a remove leaf operation to the list of operations in the
 * parent level. This operation will link the next leaf of the left leaf
 * to the next leaf of the remove leaf, reserving our linked list of
 * leaves. It will take place after the normal remove operation, so that
 * if the remove operation fails (because the item to remove does not
 * actually exist) then the leave removal does not occur.
 * <p>
 * I revisited this logic after a year and it took me a while to convince
 * myself that it was not a misunderstanding on my earlier self's part,
 * that these linked lists of otherwise empty tiers are a natural
 * occurrence.
 * <p>
 * The could be addressed by linking the inner tiers and thinking harder,
 * but that would increase the size of the project.
 */
final class MergeInner<B, A>
implements Decision<B, A>
{
    /**
     * Determine if we are deleting a final leaf in a child and therefore
     * need to lock exclusive and retreive the leaf to the left.
     *
     * @param mutation The state of the current mutation.
     * @param branch The current branch.
     * @param onlyChild The value of the last item that is about to be
     * removed from a leaf tier.
     */
    private boolean lockLeft(Mutation<B, A> mutation, Branch<B, A> branch)
    {
        if (mutation.isOnlyChild() && branch.getPivot() != null && mutation.getLeftLeaf() == null)
        {
            return mutation.getComparable().compareTo(branch.getPivot()) == 0;
        }
        return false;
    }

    /**
     * Determine if we need to merge or delete this inner tier.
     * <p>
     * We merge two branches if the child at the branch we descend can be
     * combined with either sibling. If the child can merge this method
     * returns true and a merge action is added to the parent operations.
     * <p>
     * We delete if the branch we descend has only one child. We begin to
     * mark a deletion chain. Deletion is canceled if we encounter any
     * child inner tier that has more than one child itself. We cannot
     * delete an inner tier as the result of a merge of it's children.
     * <p>
     * Additionally, if we are on a second pass after having determined
     * that we deleting the last item in a leaf tier that is an only
     * child, then we will also find and lock the leaf tier to the left of
     * the only child leaf tier. When we encounter the branch that uses
     * the item as a pivot, we'll travel to the right most leaf tier of
     * the branch to the left of the branch that uses the item as a pivot,
     * locking every level exclusively and locking the right most leaf
     * tier exclusively and noting it as the left leaf in the mutator. The
     * locks recorded in the level of the parent.
     *
     * @param mutation The state of the mutation.
     * @param levelOfParent The locks and operations of the parent.
     * @param levelOfChidl the locks and operations of child.
     * @param parent The parent tier.
     */
    public boolean test(Mutation<B, A> mutation,
                        Level<B, A> levelOfParent,
                        Level<B, A> levelOfChild,
                        InnerTier<B, A> parent)
    {
        Structure<B, A> structure = mutation.getStructure();
        TierPool<B, A> pool = structure.getPool();
        
        // Find the child tier.

        Branch<B, A> branch = parent.find(mutation.getComparable());
        InnerTier<B, A> child = pool.getInnerTier(mutation.getTxn(), branch.getAddress());

        // If we are on our way down to remove the last item of a leaf
        // tier that is an only child, then we need to find the leaf to
        // the left of the only child leaf tier. This means that we need
        // to detect the branch that uses the the value of the last item in
        // the only child leaf as a pivot. When we detect it we then
        // navigate each right most branch of the tier referenced by the
        // branch before it to find the leaf to the left of the only child
        // leaf. We then make note of it so we can link it around the only
        // child that is go be removed.

        if (lockLeft(mutation, branch))
        {
            // FIXME You need to hold these exclusive locks, so add an
            // operation that is uncancelable, but does nothing.

            int index = parent.getIndex(child.getAddress()) - 1;
            InnerTier<B, A> inner = parent;
            while (inner.getChildType() == ChildType.INNER)
            {
                inner = pool.getInnerTier(mutation.getTxn(), inner.get(index).getAddress());
                levelOfParent.lockAndAdd(inner);
                index = inner.size() - 1;
            }
            LeafTier<B, A> leaf = pool.getLeafTier(mutation.getTxn(), inner.get(index).getAddress());
            levelOfParent.lockAndAdd(leaf);
            mutation.setLeftLeaf(leaf);
        }


        // When we detect an inner tier with an only child, we note that
        // we have begun to descend a list of tiers with only one child.
        // Tiers with only one child are deleted rather than merged. If we
        // encounter a tier with children with siblings, we are no longer
        // deleting.

        if (child.size() == 1)
        {
            if (!mutation.isDeleting())
            {
                mutation.setDeleting(true);
            }
            levelOfParent.listOfOperations.add(new MergeInner.Remove<B, A>(parent, child));
            return true;
        }

        // Determine if we can merge with either sibling.

        List<InnerTier<B, A>> listToMerge = new ArrayList<InnerTier<B, A>>(2);

        int index = parent.getIndex(child.getAddress());
        if (index != 0)
        {
            InnerTier<B, A> left = pool.getInnerTier(mutation.getTxn(), parent.get(index - 1).getAddress());
            levelOfChild.lockAndAdd(left);
            levelOfChild.lockAndAdd(child);
            if (left.size() + child.size() <= structure.getInnerSize())
            {
                listToMerge.add(left);
                listToMerge.add(child);
            }
        }

        if (index == 0)
        {
            levelOfChild.lockAndAdd(child);
        }

        if (listToMerge.isEmpty() && index != parent.size() - 1)
        {
            InnerTier<B, A> right = pool.getInnerTier(mutation.getTxn(), parent.get(index + 1).getAddress());
            levelOfChild.lockAndAdd(right);
            if ((child.size() + right.size() - 1) == structure.getInnerSize())
            {
                listToMerge.add(child);
                listToMerge.add(right);
            }
        }

        // Add the merge operation.

        if (listToMerge.size() != 0)
        {
            // If the parent or ancestors have only children and we are
            // creating a chain of delete operations, we have to cancel
            // those delete operations. We cannot delete an inner tier as
            // the result of a merge, we have to allow this subtree of
            // nearly empty tiers to exist. We rewind all the operations
            // above us, but we leave the top two tiers locked exclusively.

            // FIXME I'm not sure that rewind is going to remove all the
            // operations. The number here indicates that two levels are
            // supposed to be left locked exculsive, but I don't see in
            // rewind, how the operations are removed.

            if (mutation.isDeleting())
            {
                mutation.rewind(2);
                mutation.setDeleting(false);
            }

            levelOfParent.listOfOperations.add(new MergeInner.Merge<B, A>(parent, listToMerge));

            return true;
        }

        // When we encounter an inner tier without an only child, then we
        // are no longer deleting. Returning false will cause the Query to
        // rewind the exclusive locks and cancel the delete operations, so
        // the delete action is reset.

        mutation.setDeleting(false);

        return false;
    }

    public final static class Merge<B, A>
    implements Operation<B, A>
    {
        private final InnerTier<B, A> parent;

        private final List<InnerTier<B, A>> listToMerge;

        public Merge(InnerTier<B, A> parent, List<InnerTier<B, A>> listToMerge)
        {
            this.parent = parent;
            this.listToMerge = listToMerge;
        }

        public void operate(Mutation<B, A> mutation)
        {
            InnerTier<B, A> left = listToMerge.get(0);
            InnerTier<B, A> right = listToMerge.get(1);

            int index = parent.getIndex(right.getAddress());
            Branch<B, A> branch = parent.remove(index);

            right.get(0).setPivot(branch.getPivot());
            while (right.size() != 0)
            {
                left.add(right.remove(0));
            }

            TierWriter<B, A> writer = mutation.getStructure().getWriter();
            writer.remove(right);
            writer.dirty(mutation.getTxn(), parent);
            writer.dirty(mutation.getTxn(), left);
        }

        public boolean canCancel()
        {
            return true;
        }
    }

    public final static class Remove<B, A>
    implements Operation<B, A>
    {
        private final InnerTier<B, A> parent;

        private final InnerTier<B, A> child;

        public Remove(InnerTier<B, A> parent, InnerTier<B, A> child)
        {
            this.parent = parent;
            this.child = child;
        }

        public void operate(Mutation<B, A> mutation)
        {
            int index = parent.getIndex(child.getAddress());

            parent.remove(index);
            if (parent.size() != 0)
            {
                parent.get(0).setPivot(null);
            }

            TierWriter<B, A> writer = mutation.getStructure().getWriter();
            writer.remove(child);
            writer.dirty(mutation.getTxn(), parent);
        }

        public boolean canCancel()
        {
            return true;
        }
    }
}