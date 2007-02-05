package com.agtrz.strata;

import java.util.Iterator;
import java.util.ListIterator;

public abstract class InnerTier
implements Tier
{
    // private final InnerPage page;

    protected final Strata.Structure structure;

    public InnerTier(Strata.Structure structure)
    {
        // this.page = storage.getPager().newInnerPage(storage,
        // typeOfChildren);
        this.structure = structure;
        // this.listOfBranches = new ArrayList(size + 1);
        // this.listOfBranches.add(new Branch(new LeafTier(comparator, size),
        // Branch.TERMINAL));
    }

    public Split split(Object txn, Strata.Criteria criteria)
    {
        int partition = (structure.getSize() + 1) / 2;

        Storage pager = structure.getStorage();
        InnerTier right = pager.newInnerPage(structure, txn, getTypeOfChildren());
        for (int i = partition; i < structure.getSize() + 1; i++)
        {
            right.add(remove(partition));
        }

        Branch branch = remove(getSize() - 1);
        Object pivot = branch.getObject();
        add(new Branch(branch.getKeyOfLeft(), Branch.TERMINAL, branch.getSize()));

        return new Split(pivot, right);
    }

    public boolean isFull()
    {
        return getSize() == structure.getSize();
    }

    public Branch find(Strata.Criteria criteria)
    {
        Iterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.isTerminal() || criteria.partialMatch(branch.getObject()) <= 0)
            {
                return branch;
            }
        }
        throw new IllegalStateException();
    }

    public int getIndexOfTier(Object keyOfTier)
    {
        int index = 0;
        Iterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getKeyOfLeft().equals(keyOfTier))
            {
                return index;
            }
            index++;
        }
        return -1;
    }

    public Object removeLeafTier(Object keyOfLeafTier)
    {
        Branch previous = null;
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getKeyOfLeft().equals(keyOfLeafTier))
            {
                branches.remove();
                break;
            }
            previous = branch;
        }
        return previous.getObject();
    }

    public void replacePivot(Strata.Criteria oldPivot, Object newPivot)
    {
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (oldPivot.partialMatch(branch.getObject()) == 0)
            {
                branches.set(new Branch(branch.getKeyOfLeft(), newPivot, branch.getSize()));
                break;
            }
        }
    }

    public void splitRootTier(Object txn, Split split)
    {
        Storage pager = structure.getStorage();
        InnerTier left = pager.newInnerPage(structure, txn, getTypeOfChildren());
        int count = getSize() + 1;
        for (int i = 0; i < count; i++)
        {
            left.add(remove(0));
        }
        add(new Branch(left.getKey(), split.getPivot(), left.getSize()));
        add(new Branch(split.getRight().getKey(), Branch.TERMINAL, split.getRight().getSize()));
    }

    public void replace(Tier tier, Split split)
    {
        Object keyOfTier = tier.getKey();
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getKeyOfLeft().equals(keyOfTier))
            {
                branches.set(new Branch(keyOfTier, split.getPivot(), tier.getSize()));
                branches.add(new Branch(split.getRight().getKey(), branch.getObject(), split.getRight().getSize()));
            }
        }
    }

    public boolean isLeaf()
    {
        return false;
    }

    public void copacetic(Object txn, Strata.Copacetic copacetic)
    {
        if (getSize() == 0)
        {
            throw new IllegalStateException();
        }

        Object previous = null;
        Iterator branches = listIterator();
        TierLoader loader = getTypeOfChildren() == Tier.INNER ? structure.getStorage().getInnerPageLoader() : structure.getStorage().getLeafPageLoader();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (!branches.hasNext() && !branch.isTerminal())
            {
                throw new IllegalStateException();
            }
            Tier left = loader.load(structure, txn, branch.getKeyOfLeft());
            left.copacetic(txn, copacetic);
            if (branch.getSize() != left.getSize())
            {
                throw new IllegalStateException();
            }
            if (!branch.isTerminal())
            {
                // Each key must be less than the one next to it.
                if (previous != null && structure.getCriterion().newCriteria(previous).partialMatch(branch.getObject()) >= 0)
                {
                    throw new IllegalStateException();
                }

                // Each key must occur only once in the inner tiers.
                if (!copacetic.unique(branch.getObject()))
                {
                    throw new StrataException().source(Tier.class).message("not.unqiue.in.inner.tiers");
                }
            }
            previous = branch.getObject();
        }
    }

    public boolean canMerge(Tier tier)
    {
        int index = getIndexOfTier(tier);
        if (index > 0 && get(index - 1).getSize() + get(index).getSize() <= structure.getSize())
        {
            return true;
        }
        else if (index <= structure.getSize() && get(index).getSize() + get(index + 1).getSize() <= structure.getSize())
        {
            return true;

        }
        return false;
    }

    private boolean canMerge(int indexOfLeft, int indexOfRight)
    {
        return indexOfLeft != 0 && indexOfRight != structure.getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < structure.getSize();
    }

    private TierLoader getPageLoader()
    {
        return getTypeOfChildren() == Tier.INNER ? structure.getStorage().getInnerPageLoader() : structure.getStorage().getLeafPageLoader();
    }

    private void merge(Object txn, int indexOfLeft, int indexOfRight)
    {
        Tier left = getPageLoader().load(structure, txn, get(indexOfLeft).getKeyOfLeft());
        Tier right = getPageLoader().load(structure, txn, get(indexOfRight).getKeyOfLeft());
        right.consume(txn, left, null);
    }

    public void merge(Object txn, Tier tier)
    {
        int index = getIndexOfTier(tier);
        if (canMerge(index - 1, index))
        {
            merge(txn, index - 1, index);
        }
        else if (canMerge(index, index + 1))
        {
            merge(txn, index, index + 1);
        }
    }

    public void consume(Object txn, Tier left, Object key)
    {
        InnerTier innerTier = (InnerTier) left;
        Branch oldPivot = innerTier.get(innerTier.getSize());
        shift(new Branch(oldPivot.getKeyOfLeft(), oldPivot.getObject(), oldPivot.getSize()));
        for (int i = left.getSize(); i > 0; i--)
        {
            shift(innerTier.get(i));
        }
    }

    public Tier load(Object txn, Object keyOfTier)
    {
        return getPageLoader().load(structure, txn, keyOfTier);
    }

    public abstract short getTypeOfChildren();

    public abstract void add(Branch branch);

    public abstract Branch remove(int index);

    public abstract ListIterator listIterator();

    public abstract Branch get(int index);

    public abstract void shift(Branch branch);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */