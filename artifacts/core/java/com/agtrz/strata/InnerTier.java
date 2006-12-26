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

    public InnerTier(int size)
    {
        this.size = size;
        this.listOfBranches = new ArrayList(size + 1);
        this.listOfBranches.add(new Branch(new LeafTier(size), Branch.TERMINAL));
    }

    public InnerTier(int size, List listOfBranches)
    {
        this.size = size;
        this.listOfBranches = newListOfBranches(listOfBranches);
    }

    public final static List newListOfBranches(List listOfBranches)
    {
        List newListOfBranches = new ArrayList();
        newListOfBranches.addAll(listOfBranches.subList(0, listOfBranches.size() - 1));
        Branch last = (Branch) listOfBranches.get(listOfBranches.size() - 1);
        newListOfBranches.add(new Branch(last.getLeft(), Branch.TERMINAL));
        return newListOfBranches;
    }

    public void clear()
    {
        listOfBranches.clear();
    }

    public Split split(Comparator comparator)
    {
        // This method actually throws away the current tier.
        int partition = (size + 1) / 2;

        List listOfLeft = listOfBranches.subList(0, partition);
        List listOfRight = listOfBranches.subList(partition, size + 1);

        Tier left = new InnerTier(size, listOfLeft);
        Tier right = new InnerTier(size, listOfRight);

        return new Split(((Branch) listOfLeft.get(partition - 1)).getObject(), left, right);
    }

    public boolean isFull()
    {
        return listOfBranches.size() == size + 1;
    }

    public Branch find(Comparator comparator, Object object)
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

    public void replace(Split split)
    {
        listOfBranches.clear();
        listOfBranches.add(new Branch(split.getLeft(), split.getKey()));
        listOfBranches.add(new Branch(split.getRight(), Branch.TERMINAL));
    }

    public void replace(Comparator comparator, Tier tier, Split split)
    {
        ListIterator branches = listOfBranches.listIterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            if (branch.getLeft() == tier)
            {
                branches.set(new Branch(split.getLeft(), split.getKey()));
                branches.add(new Branch(split.getRight(), branch.getObject()));
            }
        }
    }

    public boolean isLeaf()
    {
        return false;
    }

    public void copacetic(Strata.Copacetic copacetic)
    {
        TRACER.debug().record(listOfBranches, new ListFreezer(NullFreezer.INSTANCE));

        if (listOfBranches.size() == 0)
        {
            throw new IllegalStateException();
        }
        
        Comparator comparator = copacetic.getComparator();
        Object previous = null;
        Iterator branches = listOfBranches.iterator();
        while (branches.hasNext())
        {
            Branch branch = (Branch) branches.next();
            branch.getLeft().copacetic(copacetic);
            if (branch.isTerminal())
            {
                if (branches.hasNext())
                {
                    throw new IllegalStateException();
                }
            }
            else
            {
                // Each key must be less than the one next to it.
                if (previous != null && comparator.compare(previous, branch.getObject()) >= 0)
                {
                    throw new IllegalStateException();
                }
                
                // Each key must occur only once in the inner tiers.
                if (!copacetic.unique(branch.getObject()))
                {
                    throw new StrataException().source(Tier.class)
                                               .message("not.unqiue.in.inner.tiers");
                }
            }
            previous = branch.getObject();
        }
    }

    public String toString()
    {
        return listOfBranches.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */