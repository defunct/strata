package com.agtrz.strata;

import java.util.Iterator;
import java.util.ListIterator;

public abstract class InnerTier
implements Tier
{
    protected final Strata.Structure structure;

    public InnerTier(Strata.Structure structure)
    {
        this.structure = structure;
    }

    public abstract short getChildType();

    public abstract void setChildType(short childType);

    public abstract void add(Branch branch);

    public abstract Branch remove(int index);

    public abstract ListIterator listIterator();

    public abstract Branch get(int index);

    public abstract void shift(Branch branch);

    public Split split(Object txn, Strata.Criteria criteria)
    {
        int partition = (structure.getSize() + 1) / 2;

        Storage storage = structure.getStorage();
        InnerTier right = storage.newInnerTier(structure, txn, getChildType());
        for (int i = partition; i < structure.getSize() + 1; i++)
        {
            right.add(remove(partition));
        }

        Branch branch = remove(getSize());
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
        Storage storage = structure.getStorage();
        InnerTier left = storage.newInnerTier(structure, txn, getChildType());
        int count = getSize() + 1;
        for (int i = 0; i < count; i++)
        {
            left.add(remove(0));
        }
        add(new Branch(left.getKey(), split.getPivot(), left.getSize()));
        add(new Branch(split.getRight().getKey(), Branch.TERMINAL, split.getRight().getSize()));
        setChildType(Tier.INNER);
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

    public void consume(Object txn, Tier left)
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

    public void copacetic(Object txn, Strata.Copacetic copacetic)
    {
        if (getSize() < 0)
        {
            throw new IllegalStateException();
        }

        Object previous = null;
        Object lastLeftmost = null;

        Iterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (!branches.hasNext() && !branch.isTerminal())
            {
                throw new IllegalStateException();
            }

            Tier left = load(txn, branch.getKeyOfLeft());
            left.copacetic(txn, copacetic);

            if (branch.getSize() < left.getSize())
            {
                throw new IllegalStateException();
            }

            if (!branch.isTerminal())
            {
                // Each key must be less than the one next to it.
                if (previous != null && structure.compare(previous, branch.getObject()) >= 0)
                {
                    throw new IllegalStateException();
                }

                // Each key must occur only once in the inner tiers.
                if (!copacetic.unique(branch.getObject()))
                {
                    throw new IllegalStateException();
                }
            }
            previous = branch.getObject();

            Object leftmost = getLeftMost(txn, left, getChildType());
            if (lastLeftmost != null && structure.compare(lastLeftmost, leftmost) >= 0)
            {
                throw new IllegalStateException();
            }
            lastLeftmost = leftmost;
        }
    }

    private Object getLeftMost(Object txn, Tier tier, short childType)
    {
        while (childType != Tier.LEAF)
        {
            InnerTier inner = (InnerTier) tier;
            childType = inner.getChildType();
            tier = inner.load(txn, inner.get(0).getKeyOfLeft());
        }
        LeafTier leaf = (LeafTier) tier;
        return leaf.get(0);
    }

    private boolean canMerge(int indexOfLeft, int indexOfRight)
    {
        return indexOfLeft != 0 && indexOfRight != structure.getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < structure.getSize();
    }

    private TierLoader getPageLoader()
    {
        return getChildType() == Tier.INNER ? structure.getStorage().getInnerTierLoader() : structure.getStorage().getLeafTierLoader();
    }

    private void merge(Object txn, int indexOfLeft, int indexOfRight)
    {
        Tier left = getPageLoader().load(structure, txn, get(indexOfLeft).getKeyOfLeft());
        Tier right = getPageLoader().load(structure, txn, get(indexOfRight).getKeyOfLeft());
        right.consume(txn, left);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */