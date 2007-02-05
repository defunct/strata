/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.bento.Bento;
import com.agtrz.swag.io.ObjectReadBuffer;
import com.agtrz.swag.io.SizeOf;

public class BentoInnerTier
extends InnerTier
{
    private final short typeOfChildren;

    private final Bento.Address address;

    private final List listOfBranches;

    public BentoInnerTier(Strata.Structure structure, Bento.Mutator mutator, short typeOfChildren)
    {
        super(structure);
        int blockSize = SizeOf.SHORT + ((Bento.ADDRESS_SIZE * 2) + SizeOf.INTEGER) * (structure.getSize() + 1);
        this.address = mutator.allocate(blockSize).getAddress();
        this.listOfBranches = new ArrayList(structure.getSize() + 1);
        this.typeOfChildren = typeOfChildren;
    }

    public BentoInnerTier(Strata.Structure structure, Bento.Mutator mutator, Bento.Address address, Strata.ObjectLoader loader)
    {
        super(structure);
        ByteBuffer bytes = mutator.load(address).toByteBuffer();
        List listOfBranches = new ArrayList(structure.getSize() + 1);
        short typeOfChildren = bytes.getShort();
        for (int i = 0; i < bytes.getInt(); i++)
        {
            Bento.Address addressOfBranch = new Bento.Address(new ObjectReadBuffer(bytes));
            Bento.Address addressOfObject = new Bento.Address(new ObjectReadBuffer(bytes));
            int sizeOfChild = bytes.getInt();
            Branch branch;
            if (addressOfObject.getPosition() == 0L)
            {
                branch = new Branch(addressOfBranch, Branch.TERMINAL, sizeOfChild);
            }
            else
            {
                branch = new Branch(addressOfBranch, loader.load(mutator, addressOfObject), sizeOfChild);
            }
            listOfBranches.add(branch);
        }
        this.address = address;
        this.typeOfChildren = typeOfChildren;
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

    public short getTypeOfChildren()
    {
        return typeOfChildren;
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