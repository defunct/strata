package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.ListIterator;

public class MemoryTierFactory
implements TierFactory
{
    public Tier newTier(int objectSize, int airty)
    {
        return newRootTier(objectSize, airty);
    }
    
    private final static Tier newRootTier(int objectSize, int airty)
    {
        MemoryTier root = new MemoryTier(objectSize, airty, false);
        MemoryTier leaf = new MemoryTier(objectSize, airty, true);
        root.addBranch(new Branch(leaf, Branch.TERMINAL));
        return root;
    }
    
    private final static class MemoryTier
    implements Tier
    {
        public final int objectSize;
        
        public final int airty;
        
//        private final ByteBuffer memory;
        
        private final ArrayList listOfBranches;
        
        private final boolean leaf;

        public MemoryTier(int objectSize, int airty, boolean leaf)
        {
            this.objectSize = objectSize;
            this.airty = airty;
//            this.memory = ByteBuffer.allocate(objectSize * airty);
            this.listOfBranches = new ArrayList();
            this.leaf = leaf;
        }
        
        public void addBranch(Branch branch)
        {
            listOfBranches.add(branch);
        }

        public Branch insert(Stratifier stratifier, Comparator comparitor, Object object)
        {
            ListIterator branches = listOfBranches.listIterator();
            while (branches.hasNext())
            {
                Branch branch = (Branch) branches.next();
                if (branch.isTerminal()
                    || comparitor.compare(object, branch.getObject()) > 0)
                {
                    return branch;
                }   
            }
            return null;
        }
        
        public boolean isLeaf()
        {
            return leaf;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=68: */