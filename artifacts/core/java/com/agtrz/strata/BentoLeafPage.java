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
import com.agtrz.swag.util.Converter;
import com.agtrz.swag.util.Pair;

public class BentoLeafPage
extends LeafTier
{
    private final Bento.Address address;

    private Bento.Address addressOfPrevious;

    private Bento.Address addressOfNext;

    private final List listOfObjects;

    private final List listOfAddresses;

    public BentoLeafPage(Storage storage)
    {
        super(storage);
        Bento.Mutator mutator = (Bento.Mutator) storage.getStore();
        int blockSize = SizeOf.INTEGER + (Bento.ADDRESS_SIZE * 2) + (Bento.ADDRESS_SIZE * storage.getSize());
        this.address = mutator.allocate(blockSize).getAddress();
        this.listOfObjects = new ArrayList(storage.getSize());
        this.listOfAddresses = new ArrayList(storage.getSize());
    }

    public BentoLeafPage(Storage storage, Bento.Address address, Converter objectConverter)
    {
        super(storage);
        Bento.Mutator mutator = (Bento.Mutator) storage.getStore();
        Bento.Block block = mutator.load(address);
        ByteBuffer bytes = block.toByteBuffer();
        Bento.Address addressOfPrevious = new Bento.Address(new ObjectReadBuffer(bytes));
        Bento.Address addressOfNext = new Bento.Address(new ObjectReadBuffer(bytes));
        List listOfObjects = new ArrayList(storage.getSize());
        List listOfAddresses = new ArrayList(storage.getSize());
        for (int i = 0; i < storage.getSize(); i++)
        {
            Bento.Address addressOfObject = new Bento.Address(new ObjectReadBuffer(bytes));
            if (addressOfObject.getPosition() == 0)
            {
                break;
            }
            listOfAddresses.add(listOfObjects);
            listOfObjects.add(objectConverter.convert(mutator.load(addressOfObject)));
        }
        this.address = address;
        this.addressOfPrevious = addressOfPrevious;
        this.addressOfNext = addressOfNext;
        this.listOfObjects = listOfObjects;
        this.listOfAddresses = listOfAddresses;
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
        for (int i = listOfObjects.size(); i < storage.getSize(); i++)
        {
            Bento.NULL_ADDRESS.write(out);
        }
        block.write();
    }

    public int getType()
    {
        return Tier.LEAF;
    }

    public Object getKey(int index)
    {
        return listOfAddresses.get(index);
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

    public Object getPreviousLeafTier()
    {
        return addressOfPrevious;
    }

    public void setPreviousLeafTier(Object keyOfPreviousLeafTier)
    {
        this.addressOfPrevious = (Bento.Address) keyOfPreviousLeafTier;
    }

    public Object getNextLeafTier()
    {
        return addressOfNext;
    }

    public void setNextLeafTier(Object keyOfNextLeafTier)
    {
        this.addressOfNext = (Bento.Address) keyOfNextLeafTier;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */