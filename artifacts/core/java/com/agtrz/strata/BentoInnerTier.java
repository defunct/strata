/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.bento.Bento;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.SizeOf;

public class BentoInnerTier
extends InnerTier
{
    private final Bento.Address address;

    private final List listOfBranches;

    private short childType;

    public BentoInnerTier(Strata.Structure structure, Bento.Mutator mutator, short childType, int childSize)
    {
        super(structure);
        int blockSize = SizeOf.SHORT + (Bento.ADDRESS_SIZE + childSize + SizeOf.INTEGER) * (structure.getSize() + 1);
        this.address = mutator.allocate(blockSize).getAddress();
        this.listOfBranches = new ArrayList(structure.getSize() + 1);
        this.childType = childType;
    }

    public BentoInnerTier(Strata.Structure structure, Bento.Mutator mutator, Bento.Address address, ByteReader reader)
    {
        super(structure);
        ByteBuffer in = mutator.load(address).toByteBuffer();
        List listOfBranches = new ArrayList(structure.getSize() + 1);
        short typeOfChildren = in.getShort();
        int size = in.getInt();
        for (int i = 0; i < size; i++)
        {
            int sizeOfChild = in.getInt();
            Bento.Address addressOfBranch = new Bento.Address(in.getLong(), in.getInt());
            Object object = reader.read(in);
            Branch branch = null;
            if (object == null)
            {
                branch = new Branch(addressOfBranch, Branch.TERMINAL, sizeOfChild);
            }
            else
            {
                branch = new Branch(addressOfBranch, object, sizeOfChild);
            }
            listOfBranches.add(branch);
        }
        this.address = address;
        this.childType = typeOfChildren;
        this.listOfBranches = listOfBranches;
    }

    public Object getKey()
    {
        return address;
    }

    public int getSize()
    {
        return listOfBranches.size() - 1;
    }

    public short getChildType()
    {
        return childType;
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

    public String toString()
    {
        return listOfBranches.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */