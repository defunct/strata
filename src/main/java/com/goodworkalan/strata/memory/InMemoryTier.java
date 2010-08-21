package com.goodworkalan.strata.memory;

import java.util.ArrayList;
import java.util.List;

import com.goodworkalan.strata.Tier;


public class InMemoryTier<Record> extends Tier<Record, InMemoryTier<Record>> {
    private boolean childLeaf;
    
    private InMemoryTier<Record> next;

    private List<Record> records = new ArrayList<Record>();
    
    private List<InMemoryTier<Record>> children = new ArrayList<InMemoryTier<Record>>();

    public InMemoryTier() {
    }
    
    public boolean isChildLeaf() {
        return childLeaf;
    }
    
    public void setChildLeaf(boolean childLeaf) {
        this.childLeaf = childLeaf;
    }
    
    public void addRecord(int index, Record record) {
        records.add(index, record);
    }
    
    @Override
    public void addBranch(int index, Record record, InMemoryTier<Record> address) {
        records.add(index, record);
        children.add(index, address);
    }
    
    @Override
    public InMemoryTier<Record> getAddress() {
        return this;
    }
    
    @Override
    public InMemoryTier<Record> getChildAddress(int index) {
        return children.get(index);
    }

    @Override
    public InMemoryTier<Record> getNext() {
        return next;
    }

    @Override
    public int getSize() {
        return records.size();
    }

    @Override
    public Record getRecord(int index) {
        return  records.get(index);
    }
    
    @Override
    public void setRecord(int index, Record record) {
        records.set(index, record);
    }
    
    @Override
    public void setNext(InMemoryTier<Record> next) {
        this.next = next;
    }

    @Override
    public void clear(int start, int count) {
        for (int i = 0, stop = count - start; i < stop; i++) {
            records.remove(start);
            children.remove(start);
        }
    }
}
