package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class InnerTier
implements Tier
{
    protected final Strata.Structure structure;

    private final Object key;

    private short childType;

    private final List listOfBranches;

    public InnerTier(Strata.Structure structure, Object key)
    {
        this.structure = structure;
        this.key = key;
        this.listOfBranches = new ArrayList(structure.getSize() + 1);
    }

    public Object getKey()
    {
        return structure.getStorage().getKey(this);
    }

    public Object getStorageData()
    {
        return key;
    }

    public short getChildType()
    {
        return childType;
    }

    public int getSize()
    {
        return listOfBranches.size() - 1;
    }

    public void setChildType(short childType)
    {
        this.childType = childType;
    }

    public void add(Branch branch)
    {
        listOfBranches.add(branch);
    }

    public void shift(Branch branch)
    {
        listOfBranches.add(0, branch);
    }

    public Branch remove(int index)
    {
        return (Branch) listOfBranches.remove(index);
    }

    public ListIterator listIterator()
    {
        return listOfBranches.listIterator();
    }

    public int getType()
    {
        return Tier.INNER;
    }

    public Branch get(int index)
    {
        return (Branch) listOfBranches.get(index);
    }

    public Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
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
        add(new Branch(branch.getLeftKey(), Branch.TERMINAL, branch.getSize()));

        setOfDirty.add(this);
        setOfDirty.add(right);

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
            if (branch.getLeftKey().equals(keyOfTier))
            {
                return index;
            }
            index++;
        }
        return -1;
    }

    public Object removeLeafTier(Object keyOfLeafTier, Strata.TierSet setOfDirty)
    {
        Branch previous = null;
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeftKey().equals(keyOfLeafTier))
            {
                branches.remove();
                setOfDirty.add(this);
                break;
            }
            previous = branch;
        }
        return previous.getObject();
    }

    public void replacePivot(Strata.Criteria oldPivot, Object newPivot, Strata.TierSet setOfDirty)
    {
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (oldPivot.partialMatch(branch.getObject()) == 0)
            {
                branches.set(new Branch(branch.getLeftKey(), newPivot, branch.getSize()));
                setOfDirty.add(this);
                break;
            }
        }
    }

    public final void splitRootTier(Object txn, Split split, Strata.TierSet setOfDirty)
    {
        Storage storage = structure.getStorage();
        InnerTier left = storage.newInnerTier(structure, txn, getChildType());
        int count = getSize() + 1;
        for (int i = 0; i < count; i++)
        {
            left.add(remove(0));
        }
        setOfDirty.add(left);

        add(new Branch(left.getKey(), split.getPivot(), left.getSize()));
        add(new Branch(split.getRight().getKey(), Branch.TERMINAL, split.getRight().getSize()));
        setChildType(Tier.INNER);

        setOfDirty.add(this);
    }

    public void replace(Tier tier, Split split, Strata.TierSet setOfDirty)
    {
        Object keyOfTier = tier.getKey();
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeftKey().equals(keyOfTier))
            {
                branches.set(new Branch(keyOfTier, split.getPivot(), tier.getSize()));
                branches.add(new Branch(split.getRight().getKey(), branch.getObject(), split.getRight().getSize()));
                setOfDirty.add(this);
                break;
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

    public boolean merge(Object txn, Tier tier, Strata.TierSet setOfDirty)
    {
        int index = getIndexOfTier(tier.getKey());
        if (canMerge(index - 1, index))
        {
            merge(txn, index - 1, index, setOfDirty);
            return true;
        }
        else if (canMerge(index, index + 1))
        {
            merge(txn, index, index + 1, setOfDirty);
            return true;
        }
        return false;
    }

    public void consume(Object txn, Tier left, Strata.TierSet setOfDirty)
    {
        InnerTier inner = (InnerTier) left;

        Branch oldPivot = inner.get(inner.getSize());
        shift(new Branch(oldPivot.getLeftKey(), oldPivot.getObject(), oldPivot.getSize()));

        for (int i = left.getSize(); i > 0; i--)
        {
            shift(inner.get(i));
        }

        setOfDirty.add(this);
        structure.getStorage().free(structure, txn, inner);
    }

    public void revert(Object txn)
    {
        structure.getStorage().revert(structure, txn, this);
    }

    public void write(Object txn)
    {
        structure.getStorage().write(structure, txn, this);
    }

    public void copacetic(Object txn, Strata.Copacetic copacetic)
    {
        if (getSize() < 0)
        {
            throw new IllegalStateException();
        }

        Object previous = null;
        Object lastLeftmost = null;

        Comparator comparator = structure.newComparator(txn);
        Iterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (!branches.hasNext() && !branch.isTerminal())
            {
                throw new IllegalStateException();
            }

            Tier left = getTier(txn, branch.getLeftKey());
            left.copacetic(txn, copacetic);

            if (branch.getSize() < left.getSize())
            {
                throw new IllegalStateException();
            }

            if (!branch.isTerminal())
            {
                // Each key must be less than the one next to it.
                if (previous != null && comparator.compare(previous, branch.getObject()) >= 0)
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
            if (lastLeftmost != null && comparator.compare(lastLeftmost, leftmost) >= 0)
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
            tier = inner.getTier(txn, inner.get(0).getLeftKey());
        }
        LeafTier leaf = (LeafTier) tier;
        return leaf.get(0);
    }

    private boolean canMerge(int indexOfLeft, int indexOfRight)
    {
        return indexOfLeft >= 0 && indexOfRight <= getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < structure.getSize() + 1;
    }

    public Tier getTier(Object txn, Object key)
    {
        if (getChildType() == Tier.INNER)
            return structure.getStorage().getInnerTier(structure, txn, key);
        return structure.getStorage().getLeafTier(structure, txn, key);
    }

    private void merge(Object txn, int indexOfLeft, int indexOfRight, Strata.TierSet setOfDirty)
    {
        Tier left = getTier(txn, get(indexOfLeft).getLeftKey());
        Tier right = getTier(txn, get(indexOfRight).getLeftKey());

        right.consume(txn, left, setOfDirty);

        get(indexOfRight).setSize(right.getSize());
        remove(indexOfLeft);

        setOfDirty.add(this);
    }
    
    public String toString()
    {
        return listOfBranches.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */