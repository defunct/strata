/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata.bento;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.bento.Bento;
import com.agtrz.strata.LeafTier;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Structure;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.SizeOf;

public class BentoLeafTier
extends LeafTier
{
    private final Bento.Address address;

    private final List listOfObjects;

    private Bento.Address addressOfPrevious;

    private Bento.Address addressOfNext;

    public BentoLeafTier(Strata.Structure structure, Bento.Mutator mutator, int objectSize)
    {
        super(structure);
        int blockSize = SizeOf.INTEGER + (Bento.ADDRESS_SIZE * 2) + (objectSize * structure.getSize());
        this.address = mutator.allocate(blockSize).getAddress();
        this.listOfObjects = new ArrayList(structure.getSize());
        this.addressOfPrevious = Bento.NULL_ADDRESS;
        this.addressOfNext = Bento.NULL_ADDRESS;
    }

    public BentoLeafTier(Strata.Structure structure, Bento.Mutator mutator, Bento.Address address, ByteReader reader)
    {
        super(structure);
        Bento.Block block = mutator.load(address);
        ByteBuffer in = block.toByteBuffer();
        Bento.Address addressOfPrevious = new Bento.Address(in.getLong(), in.getInt());
        Bento.Address addressOfNext = new Bento.Address(in.getLong(), in.getInt());
        List listOfObjects = new ArrayList(structure.getSize());
        for (int i = 0; i < structure.getSize(); i++)
        {
            Object object = reader.read(in);
            if (object == null)
            {
                break;
            }
            listOfObjects.add(object);
        }
        this.address = address;
        this.addressOfPrevious = addressOfPrevious;
        this.addressOfNext = addressOfNext;
        this.listOfObjects = listOfObjects;
    }

        
    public void revert(Structure structure, Object txn)
    {
        BentoStorage storage = (BentoStorage) structure.getStorage();
        storage.revert(address);
    }
    
    public Object getKey()
    {
        return address;
    }

    public int getSize()
    {
        return listOfObjects.size();
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