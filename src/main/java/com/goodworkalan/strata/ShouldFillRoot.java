package com.goodworkalan.strata;

/**
 * Determine if the root inner tier should be filled with contents of a single
 * remaining child inner tier. The root inner tier is filled with the contents
 * of a single child when the single child is merged from two remaining
 * children. When an root inner tier has only one inner tier child, the contents
 * of that inner tier child becomes the root of the b-tree.
 * 
 * @author Alan Gutierrez
 * 
 * @param <T>
 *            The value type of the b+tree objects.
 * @param <A>
 *            The address type used to identify an inner or leaf tier.
 */
final class ShouldFillRoot<T, A>
implements RootDecision<T, A> {
    /**
     * Determine if the root inner tier has only two remaining children and if
     * those children are going to merge to form a single remaining child.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     * @return True if the operations performed by this root decision are
     *         applicable.
     */
    public boolean test(Mutation<T, A> mutation, Level<T, A> rootLevel, Tier<T, A> root) {
        if (!root.isChildLeaf() && root.getSize() == 2) {
            Structure<T, A> structure = mutation.getStructure();
            Tier<T, A> first = structure.getStorage().load(mutation.getStash(), root.getChildAddress(0));
            Tier<T, A> second = structure.getStorage().load(mutation.getStash(), root.getChildAddress(1));
            // FIXME These numbers are off.
            return first.getSize() + second.getSize() == structure.getInnerSize();
        }
        return false;
    }

    /**
     * Add the fill root operation to the list of operations in the per level
     * mutation state container.
     * 
     * @param mutation
     *            The mutation state container.
     * @param rootLevel
     *            The per level mutation state for the root level.
     * @param root
     *            The root inner tier.
     */
    public void operation(Mutation<T, A> mutation, Level<T, A> rootLevel, Tier<T, A> root) {
        rootLevel.operations.add(new FillRoot<T, A>(root));
    }
}