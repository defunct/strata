/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.swag.util.IdentityObject;

public class ArrayListInnerTier
extends InnerTier
{
    private final short typeOfChildren;

    private final List listOfBranches;

    public ArrayListInnerTier(Strata.Structure structure, short typeOfChildren)
    {
        super(structure);
        this.listOfBranches = new ArrayList(structure.getSize() + 1);
        this.typeOfChildren = typeOfChildren;
    }

    public Object getKey()
    {
        return new IdentityObject(this);
    }

    public int getSize()
    {
        return listOfBranches.size() - 1;
    }

    public short getTypeOfChildren()
    {
        return typeOfChildren;
    }

    public Branch get(int index)
    {
        return (Branch) listOfBranches.get(index);
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
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */