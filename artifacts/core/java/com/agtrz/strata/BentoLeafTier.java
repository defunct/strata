/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.bento.Bento;
import com.agtrz.swag.io.ObjectReadBuffer;
import com.agtrz.swag.io.ObjectWriteBuffer;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.Pair;

public class BentoLeafTier
extends LeafTier
{
    private final Bento.Address address;

    private final List listOfObjects;

    private Bento.Address addressOfPrevious;

    private Bento.Address addressOfNext;

    public BentoLeafTier(Strata.Structure structure, Bento.Mutator mutator)
    {
        super(structure);
        int blockSize = SizeOf.INTEGER + (Bento.ADDRESS_SIZE * 2) + (Bento.ADDRESS_SIZE * structure.getSize());
        this.address = mutator.allocate(blockSize).getAddress();
        this.listOfObjects = new ArrayList(structure.getSize());
        this.addressOfPrevious = Bento.NULL_ADDRESS;
        this.addressOfNext = Bento.NULL_ADDRESS;
    }

    public BentoLeafTier(Strata.Structure structure, Bento.Mutator mutator, Bento.Address address, BentoStorage.ObjectLoader blockLoader)
    {
        super(structure);
        Bento.Block block = mutator.load(address);
        ByteBuffer bytes = block.toByteBuffer();
        Bento.Address addressOfPrevious = new Bento.Address(new ObjectReadBuffer(bytes));
        Bento.Address addressOfNext = new Bento.Address(new ObjectReadBuffer(bytes));
        List listOfObjects = new ArrayList(structure.getSize());
        for (int i = 0; i < structure.getSize(); i++)
        {
            Bento.Address addressOfObject = new Bento.Address(new ObjectReadBuffer(bytes));
            if (addressOfObject.getPosition() == 0)
            {
                break;
            }
            listOfObjects.add(blockLoader.load(mutator, addressOfObject));
        }
        this.address = address;
        this.addressOfPrevious = addressOfPrevious;
        this.addressOfNext = addressOfNext;
        this.listOfObjects = listOfObjects;
    }

    public Object getKey()
    {
        return address;
    }

    public int getSize()
    {
        return listOfObjects.size();
    }

    public void write(Object store)
    {
        Bento.Mutator mutator = (Bento.Mutator) store;
        Bento.Block block = mutator.load(address);
        ByteBuffer bytes = block.toByteBuffer();
        ObjectWriteBuffer out = new ObjectWriteBuffer(bytes);
        addressOfPrevious.write(out);
        addressOfNext.write(out);
        for (int i = 0; i < listOfObjects.size(); i++)
        {
            Pair pair = (Pair) listOfObjects.get(i);
            Bento.Address address = (Bento.Address) pair.getKey();
            address.write(out);
        }
        for (int i = listOfObjects.size(); i < structure.getSize(); i++)
        {
            Bento.NULL_ADDRESS.write(out);
        }
        block.write();
    }

    public Object get(int index)
    {
        return listOfObjects.get(index);
    }

    public String toString()
    {
        return listOfObjects.toString();
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

    public ListIterator listIterator()
    {
        return listOfObjects.listIterator();
    }

    public Object getPreviousLeafKey()
    {
        return addressOfPrevious;
    }

    public void setPreviousLeafKey(Object previousLeafKey)
    {
        this.addressOfPrevious = (Bento.Address) previousLeafKey;
    }

    public Object getNextLeafKey()
    {
        return addressOfNext;
    }

    public void setNextLeafKey(Object nextLeafKey)
    {
        this.addressOfNext = (Bento.Address) nextLeafKey;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */