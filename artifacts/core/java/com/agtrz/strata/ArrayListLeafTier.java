/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.swag.util.IdentityObject;

public class ArrayListLeafTier
extends LeafTier
{
    private Object previousLeafKey;

    private Object nextLeafKey;

    private final List listOfObjects;

    public ArrayListLeafTier(Strata.Structure structure)
    {
        super(structure);
        this.listOfObjects = new ArrayList(structure.getSize());
    }
    
    public Object getKey()
    {
        return new IdentityObject(this);
    }

    public int getSize()
    {
        return listOfObjects.size();
    }

    public ListIterator listIterator()
    {
        return listOfObjects.listIterator();
    }

    public int getType()
    {
        return Tier.LEAF;
    }

    public Object get(int index)
    {
        return listOfObjects.get(index);
    }

    public Object remove(int index)
    {
        return listOfObjects.remove(index);
    }

    public void add(Object object)
    {
        listOfObjects.add(object);
    }

    public void shift(Object object)
    {
        listOfObjects.add(0, object);
    }

    public Object getPreviousLeafKey()
    {
        return previousLeafKey;
    }

    public void setPreviousLeafKey(Object previousLeafKey)
    {
        this.previousLeafKey = previousLeafKey;
    }

    public Object getNextLeafKey()
    {
        return nextLeafKey;
    }

    public void setNextLeafKey(Object nextLeafKey)
    {
        this.nextLeafKey = nextLeafKey;
    }
    
    public String toString()
    {
        return listOfObjects.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */