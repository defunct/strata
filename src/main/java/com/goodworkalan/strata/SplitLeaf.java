package com.goodworkalan.strata;

import static com.goodworkalan.strata.Leaves.link;

/**
 * Split a leaf into two leaves using an index value close to the middle as the
 * pivot. When chosing a split location, we need to be certain that we don't
 * split a string of duplicate indexing values. This method will make sure the
 * split location references an object unique indexing value or the first object
 * in a string of objects with identical indexing values.
 * <p>
 * This split operation is for the common case of an insert into a leaf that is
 * not part of a linked list of b+tree leaves of duplicate index values
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class SplitLeaf<T, A>
implements Operation<T, A> {
    /** The parent inner tier of the child leaf tier to split. */ 
    private final Tier<T, A> inner;

    /**
     * Create a leaf split operation.
     * 
     * @param inner
     *            The parent inner tier of the child leaf tier to split.
     */
    public SplitLeaf(Tier<T, A> inner) {
        this.inner = inner;
    }

    /**
     * Split the leaf.
     * 
     * @param mutation
     *            The mutation state container.
     */
    public void operate(Mutation<T, A> mutation) {
        // Get the collection of the core services of the b+tree.
        Structure<T, A> structure = mutation.getStructure();

        // Find the branch that navigates to the leaf child.
        int branch = inner.find(mutation.getComparable());
        Tier<T, A> leaf = structure.getPool().get(mutation.getStash(), inner.getChildAddress(branch));

        // The leaf may contain duplicate index values. We need to make sure
        // that when we split the leaf we do not split a string of duplicate
        // index values. We create a comparator for the middle value and then
        // search in increments backward and forward in the leaf tier to find an
        // index value that is not equal to the middle value.

        int middle = leaf.getSize() >> 1;
        boolean odd = (leaf.getSize() & 1) == 1;
        int lesser = middle - 1;
        int greater = odd ? middle + 1 : middle;

        int partition = -1;
        Comparable<? super T> candidate = mutation.getStructure().getComparableFactory().newComparable(mutation.getStash(), leaf.getRecord(middle));
        for (int i = 0; partition == -1 && i < middle; i++) {
            // If we first fild an unequal value at a lesser index, then we
            // split after that index and the middle value becomes the first
            // value in a new leaf tier. If we first find an unequal value at a
            // greater index, then the value at the greater index becomes the
            // first value in a new leaf tier.

            if (candidate.compareTo(leaf.getRecord(lesser)) != 0) {
                partition = lesser + 1;
            } else if (candidate.compareTo(leaf.getRecord(greater)) != 0) {
                partition = greater;
            }

            lesser--;
            greater++;
        }

        // Create a new tier to the right of the leaf tier.
        Tier<T, A> right = mutation.newLeafTier();
        link(mutation, leaf, right);

        // Copy the values at and after the partition into the new right tier.
        for (int i = partition, stop = leaf.getSize(); i < stop; i++) {
            right.addRecord(leaf.getSize(), leaf.getRecord(i));
        }
        leaf.clear(partition, leaf.getSize() - partition);

        // Add a branch for the the new leaf in the parent inner tier. 
        int index = inner.getIndexOfChildAddress(leaf.getAddress());
        inner.addBranch(index + 1, right.getRecord(0), right.getAddress());

        // Stage the dirty tiers for write.
        Stage<T, A> stage = structure.getStage();
        stage.dirty(mutation.getStash(), inner);
        stage.dirty(mutation.getStash(), leaf);
        stage.dirty(mutation.getStash(), right);
    }

    /**
     * Return true indicating that this is a split operation.
     * 
     * @return True indicating that this is a split operation.
     */
    public boolean isSplitOrMerge() {
        return true;
    }
}
