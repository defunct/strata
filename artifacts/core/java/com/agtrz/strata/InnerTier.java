package com.agtrz.strata;

import java.util.Iterator;
import java.util.ListIterator;

public abstract class InnerTier
implements Tier
{
    // private final InnerPage page;

    protected final Storage storage;

    public InnerTier(Storage storage)
    {
        // this.page = storage.getPager().newInnerPage(storage,
        // typeOfChildren);
        this.storage = storage;
        // this.listOfBranches = new ArrayList(size + 1);
        // this.listOfBranches.add(new Branch(new LeafTier(comparator, size),
        // Branch.TERMINAL));
    }

    public Split split(Object object, Object keyOfObject)
    {
        int partition = (storage.getSize() + 1) / 2;

        Pager pager = storage.getPager();
        InnerTier right = pager.newInnerPage(storage, getTypeOfChildren());
        for (int i = partition; i < storage.getSize() + 1; i++)
        {
            right.add(remove(partition));
        }

        Branch branch = remove(getSize() - 1);
        Object pivot = branch.getObject();
        Object keyOfPivot = branch.getKeyOfObject();
        add(new Branch(branch.getKeyOfLeft(), pager.getNullKey(), Branch.TERMINAL, branch.getSize()));

        return new Split(pivot, keyOfPivot, right);
    }

    public boolean isFull()
    {
        return getSize() == storage.getSize();
    }

    public Branch find(Object object)
    {
        Iterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.isTerminal() || storage.getComparator().compare(object, branch.getObject()) <= 0)
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

    public void replacePivot(Object oldPivot, Object keyOfNewPivot, Object newPivot)
    {
        ListIterator branches = listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (storage.getComparator().compare(branch.getObject(), oldPivot) == 0)
            {
                branches.set(new Branch(branch.getKeyOfLeft(), keyOfNewPivot, newPivot, branch.getSize()));
                break;
            }
        }
    }

    public void splitRootTier(Split split)
    {
        Pager pager = storage.getPager();
        InnerTier left = pager.newInnerPage(storage, getTypeOfChildren());
        int count = getSize() + 1;
        for (int i = 0; i < count; i++)
        {
            left.add(remove(0));
        }
        add(new Branch(left.getKey(), split.getKeyOfPivot(), split.getPivot(), left.getSize()));
        add(new Branch(split.getRight().getKey(), pager.getNullKey(), Branch.TERMINAL, split.getRight().getSize()));
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
                branches.set(new Branch(keyOfTier, split.getKeyOfPivot(), split.getPivot(), tier.getSize()));
                branches.add(new Branch(split.getRight().getKey(), branch.getKeyOfObject(), branch.getObject(), split.getRight().getSize()));
            }
        }
    }

    public boolean isLeaf()
    {
        return false;
    }

    public void copacetic(Strata.Copacetic copacetic)
    {
        if (getSize() == 0)
        {
            throw new IllegalStateException();
        }

        Object previous = null;
        Iterator branches = listIterator();
        PageLoader loader = getTypeOfChildren() == Tier.INNER ? storage.getPager().getInnerPageLoader() : storage.getPager().getLeafPageLoader();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (!branches.hasNext() && !branch.isTerminal())
            {
                throw new IllegalStateException();
            }
            Tier left = loader.load(storage, branch.getKeyOfLeft());
            left.copacetic(copacetic);
            if (branch.getSize() != left.getSize())
            {
                throw new IllegalStateException();
            }
            if (!branch.isTerminal())
            {
                // Each key must be less than the one next to it.
                if (previous != null && storage.getComparator().compare(previous, branch.getObject()) >= 0)
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
        if (index > 0 && get(index - 1).getSize() + get(index).getSize() <= storage.getSize())
        {
            return true;
        }
        else if (index <= storage.getSize() && get(index).getSize() + get(index + 1).getSize() <= storage.getSize())
        {
            return true;

        }
        return false;
    }

    private boolean canMerge(int indexOfLeft, int indexOfRight)
    {
        return indexOfLeft != 0 && indexOfRight != storage.getSize() && get(indexOfLeft).getSize() + get(indexOfRight).getSize() < storage.getSize();
    }

    private PageLoader getPageLoader()
    {
        return getTypeOfChildren() == Tier.INNER ? storage.getPager().getInnerPageLoader() : storage.getPager().getLeafPageLoader();
    }

    private void merge(int indexOfLeft, int indexOfRight)
    {
        Tier left = getPageLoader().load(storage, get(indexOfLeft).getKeyOfLeft());
        Tier right = getPageLoader().load(storage, get(indexOfRight).getKeyOfLeft());
        right.consume(left, null);
    }

    public void merge(Tier tier)
    {
        int index = getIndexOfTier(tier);
        if (canMerge(index - 1, index))
        {
            merge(index - 1, index);
        }
        else if (canMerge(index, index + 1))
        {
            merge(index, index + 1);
        }
    }

    public void consume(Tier left, Object key)
    {
        InnerTier innerTier = (InnerTier) left;
        Branch oldPivot = innerTier.get(innerTier.getSize());
        shift(new Branch(oldPivot.getKeyOfLeft(), oldPivot.getKeyOfObject(), oldPivot.getObject(), oldPivot.getSize()));
        for (int i = left.getSize(); i > 0; i--)
        {
            shift(innerTier.get(i));
        }
    }
    
    public Tier load(Object keyOfTier)
    {
        return getPageLoader().load(storage, keyOfTier);
    }

    public abstract short getTypeOfChildren();

    public abstract void add(Branch branch);

    public abstract Branch remove(int index);

    public abstract ListIterator listIterator();

    public abstract Branch get(int index);

    public abstract void shift(Branch branch);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */