package com.goodworkalan.strata;

import java.util.ArrayList;
import java.util.List;

public class CharacterTier extends Tier<Character, CharacterTier> {
    private final List<Character> records = new ArrayList<Character>();
    
    private final List<CharacterTier> children = new ArrayList<CharacterTier>();
    
    private CharacterTier next;
    
    private boolean childLeaf;

    public CharacterTier() {
    }
    
    @Override
    public void addBranch(int index, Character record, CharacterTier address) {
        records.add(index, record);
        children.add(index, address);
    }
    
    @Override
    public void addRecord(int index, Character record) {
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
    public CharacterTier getAddress() {
        return this;
    }
    
    public CharacterTier getChildAddress(int index) {
        return children.get(index);
    }
    
    @Override
    public CharacterTier getNext() {
        return next;
    }
    
    @Override
    public Character getRecord(int index) {
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
    
    public void setNext(CharacterTier next) {
        this.next = next;
    }
    
    @Override
    public void setRecord(int index, Character record) {
        records.set(index, record);
    }
}
