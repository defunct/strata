package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.swag.trace.ListFreezer;
import com.agtrz.swag.trace.NullFreezer;
import com.agtrz.swag.trace.Tracer;
import com.agtrz.swag.trace.TracerFactory;

public class InnerTier
implements Tier
{
    private final static Tracer TRACER = TracerFactory.INSTANCE.getTracer(Tier.class);

    private final int size;

    private final List listOfBranches;

    private final Comparator comparator;

    public InnerTier(Comparator comparator, int size)
    {
        this.size = size;
        this.comparator = comparator;
        this.listOfBranches = new ArrayList(size + 1);
        this.listOfBranches.add(new Branch(new LeafTier(comparator, size), Branch.TERMINAL));
    }

    public InnerTier(Comparator comparator, int size, List listOfBranches)
    {
        this.size = size;
        this.comparator = comparator;
        this.listOfBranches = listOfBranches;
    }

    public int size()
    {
        return listOfBranches.size() - 1;
    }

    public void clear()
    {
        listOfBranches.clear();
    }

    public Split split(Object object)
    {
        int partition = (size + 1) / 2;

        List listOfRight = new ArrayList(size + 1);
        for (int i = partition; i < size + 1; i++)
        {
            listOfRight.add(listOfBranches.remove(partition));
        }

        Branch branch = (Branch) listOfBranches.remove(listOfBranches.size() - 1);
        Object pivot = branch.getObject();
        listOfBranches.add(new Branch(branch.getLeft(), Branch.TERMINAL));

        Tier right = new InnerTier(comparator, size, listOfRight);

        return new Split(pivot, right);
    }

    public boolean isFull()
    {
        return listOfBranches.size() == size + 1;
    }

    public Branch find(Object object)
    {
        ListIterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.isTerminal() || comparator.compare(object, branch.getObject()) <= 0)
            {
                return branch;
            }
        }
        throw new IllegalStateException();
    }

    public int getIndexOfTier(Tier tier)
    {
        int index = 0;
        Iterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeft() == tier)
            {
                return index;
            }
            index++;
        }
        return -1;
    }
    public Object removeLeafTier(LeafTier leafTier)
    {
        Branch previous = null;
        ListIterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeft() == leafTier)
            {
                branches.remove();
                break;
            }
            previous = branch;
        }
        return previous.getObject();
    }

    public void replacePivot(Object oldPivot, Object newPivot)
    {
        ListIterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (comparator.compare(branch.getObject(), oldPivot) == 0)
            {
                branches.set(new Branch(branch.getLeft(), newPivot));
                break;
            }
        }
    }

    public void splitRootTier(Split split)
    {
        List listOfLeft = new ArrayList(size + 1);
        listOfLeft.addAll(listOfBranches);
        Tier left = new InnerTier(comparator, size, listOfLeft);
        listOfBranches.clear();
        listOfBranches.add(new Branch(left, split.getKey()));
        listOfBranches.add(new Branch(split.getRight(), Branch.TERMINAL));
    }

    public void replace(Tier tier, Split split)
    {
        ListIterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeft() == tier)
            {
                branches.set(new Branch(tier, split.getKey()));
                branches.add(new Branch(split.getRight(), branch.getObject()));
            }
        }
    }

    public boolean isLeaf()
    {
        return false;
    }

    public Branch getBranch(int index)
    {
        return (Branch) listOfBranches.get(index);
    }

    public void copacetic(Strata.Copacetic copacetic)
    {
        TRACER.debug().record(listOfBranches, new ListFreezer(NullFreezer.INSTANCE));

        if (listOfBranches.size() == 0)
        {
            throw new IllegalStateException();
        }

        Object previous = null;
        Iterator branches = listOfBranches.iterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (!branches.hasNext() && !branch.isTerminal())
            {
                throw new IllegalStateException();
            }
            branch.getLeft().copacetic(copacetic);
            if (branch.getCount() != branch.getLeft().size())
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
                    throw new StrataException().source(Tier.class).message("not.unqiue.in.inner.tiers");
                }
            }
            previous = branch.getObject();
        }
    }

    public String toString()
    {
        return listOfBranches.toString();
    }

    public boolean canMerge(Tier childTier)
    {
        return false;
    }

    public void merge(Tier childTier)
    {
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */