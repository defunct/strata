package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.List;

public class IntegerTier extends Tier<Integer, IntegerTier> {
    private final List<Integer> records = new ArrayList<Integer>();
    
    private final List<IntegerTier> children = new ArrayList<IntegerTier>();
    
    private IntegerTier next;
    
    private boolean childLeaf;

    public IntegerTier() {
    }
    
    @Override
    public void addBranch(int index, Integer record, IntegerTier address) {
        records.add(index, record);
        children.add(index, address);
    }
    
    @Override
    public void addRecord(int index, Integer record) {
        records.add(index, record);
    }
    
    public void clear(int start, int count) {
        while (count-- != 0) {
            records.remove(start);
            if (children.size() != 0) {
                children.remove(start);
            }
        }
    }
    
    @Override
    public IntegerTier getAddress() {
        return this;
    }
    
    public IntegerTier getChildAddress(int index) {
        return children.get(index);
    }
    
    @Override
    public IntegerTier getNext() {
        return next;
    }
    
    @Override
    public Integer getRecord(int index) {
        return records.get(index);
    }
    
    @Override
    public int getSize() {
        return records.size();
    }
    
    public boolean isChildLeaf() {
        return childLeaf;
    }
    
    @Override
    public void setChildLeaf(boolean leaf) {
        this.childLeaf = leaf;
    }
    
    public void setNext(IntegerTier next) {
        this.next = next;
    }
    
    @Override
    public void setRecord(int index, Integer record) {
        records.set(index, record);
    }
}
