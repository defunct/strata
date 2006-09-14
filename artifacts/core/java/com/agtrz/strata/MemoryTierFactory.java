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

public class MemoryTierFactory
implements TierFactory
{
    private final static Tracer TRACER = TracerFactory.INSTANCE.getTracer(Tier.class);
    
    public InnerTier newRootTier(int objectSize, int size)
    {
        return new MemoryInnerTier(size);
    }
    
    private final static class MemoryInnerTier
    implements InnerTier
    {
        private final int size;
        
        private final List listOfBranches;
        
        public MemoryInnerTier(int size)
        {
            this.size = size;
            this.listOfBranches = new ArrayList(size + 1);
            this.listOfBranches.add(new Branch(new MemoryLeafTier(size), Branch.TERMINAL));
        }

        public MemoryInnerTier(int size, List listOfBranches)
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
            
            Tier left = new MemoryInnerTier(size, listOfLeft);
            Tier right = new MemoryInnerTier(size, listOfRight);
            
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
                if (branch.isTerminal()
                    || comparator.compare(object, branch.getObject()) <= 0)
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
        
        public void copacetic(Comparator comparator)
        {
            TRACER.debug()
                  .record(listOfBranches,
                          new ListFreezer(NullFreezer.INSTANCE));
            
            if (listOfBranches.size() == 0)
            {
                throw new IllegalStateException();
            }

            Object previous = null;
            Iterator branches = listOfBranches.iterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                branch.getLeft().copacetic(comparator);
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
                    if
                    (
                        previous != null
                        && comparator.compare(previous, branch.getObject()) >= 0
                    )
                    {
                        throw new IllegalStateException();
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

    private final static class MemoryLeafTier
    implements LeafTier
    {
        private final int size;

        private final List listOfObjects;
        
        public MemoryLeafTier(int size)
        {
            List listOfObjects = new ArrayList();
            listOfObjects.add(Branch.TERMINAL);
            this.size = size;
            this.listOfObjects = listOfObjects;
        }
        
        public MemoryLeafTier(int size, List listOfObjects)
        {
            this.size = size;
            this.listOfObjects = new ArrayList(listOfObjects);
            this.listOfObjects.add(Branch.TERMINAL);
        }
        
        public void clear()
        {
            listOfObjects.clear();
        }
        
        public boolean isFull()
        {
            return listOfObjects.size() == size + 1;
        }
        
        public void remove(Tier tier)
        {
            throw new UnsupportedOperationException();
        }

        public Split split(Comparator comparator)
        {
            // This method actually throws away the current tier.
//            int partition = listOfObjects.size() / 2;
            int partition = size / 2;
            
            List listOfLeft = listOfObjects.subList(0, partition);
            List listOfRight = listOfObjects.subList(partition, size);
            
            Tier left = new MemoryLeafTier(size, listOfLeft);
            Tier right = new MemoryLeafTier(size, listOfRight);
            
            return new Split(listOfLeft.get(listOfLeft.size() - 1), left, right);
        }
        
        public void insert(Comparator comparator, Object object)
        {
            assert !isFull();
            ListIterator objects = listOfObjects.listIterator();
            while (objects.hasNext())
            {
                Object before = objects.next();
                if (before == Branch.TERMINAL
                    || comparator.compare(object, before) <= 0)
                {
                    objects.previous();
                    objects.add(object);
                    break;
                }   
            }
        }
        
        public Object find(Comparator comparator, Object object)
        {
            ListIterator objects = listOfObjects.listIterator();
            while (objects.hasNext())
            {
                Object before = objects.next();
                if (before == Branch.TERMINAL)
                {
                    return null;
                }
                else if (comparator.compare(object, before) == 0)
                {
                    return object;
                }   
            }
            throw new IllegalStateException();
        }
 
        public boolean isLeaf()
        {
            return true;
        }

        public void copacetic(Comparator comparator)
        {
            TRACER.debug()
                  .record(listOfObjects, new ListFreezer(NullFreezer.INSTANCE));
            if (listOfObjects.size() < 2)
            {
                throw new IllegalStateException();
            }
            Object previous = null;
            Iterator objects = listOfObjects.iterator();
            while (objects.hasNext())
            {
                Object object = objects.next();
                if (object == Branch.TERMINAL)
                {
                    if (objects.hasNext())
                    {
                        throw new IllegalStateException();
                    }
                }
                else if
                (
                    previous != null
                    && comparator.compare(previous, object) > 0
                )
                {
                    throw new IllegalStateException();
                }
                previous = object;
            }
        }
        
        public String toString()
        {
            return listOfObjects.toString();
        }
    }
 }

/* vim: set et sw=4 ts=4 ai tw=68: */