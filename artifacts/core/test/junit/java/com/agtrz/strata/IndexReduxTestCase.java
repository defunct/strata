/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata;

import java.awt.Dimension;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.WeakHashMap;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTree;
import javax.swing.SwingUtilities;
import javax.swing.event.EventListenerList;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import junit.framework.TestCase;

/**
 * Things to remember, well, the search path you've followed is implicit
 * in the string and index.
 * 
 * @author Alan Gutierez
 */
public class IndexReduxTestCase
extends TestCase
{
    private final static String[] ALPHABET = new String[]
    {                                        
        "alpha",
        "beta",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        "juliet",
        "kilo",
        "lima",
        "mike",
        "november",
        "oscar",
        "papa",
        "quebec",
        "romeo",
        "sierra",
        "tango",
        "uniform",
        "victor",
        "whisky",
        "yankee",
        "x-ray",
        "zebra"
    };

    private final static class Tier
    {
        public final char ch;
        public final int index;
        public final Object[] children;
        
        public Tier(char character, int index, Object[] children)
        {
            this.ch = character;
            this.index = index;
            this.children = children;
        }
        
        public String toString()
        {
            return "(" + (ch == Character.MIN_VALUE ? "MIX_VALUE" :  new Character(ch).toString()) + ":" + index + ") "; // + Arrays.asList(children).toString();
        }
    }
    
    private static void insertTest(Tier[] tiers, String string)
    {
        insert(tiers, string);
        assertTrue(contains(tiers, string));
    }
    
    private static void insertTest(Tier[] tiers, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            insertTest(tiers, strings[i]);
        }
        
        containsTest(tiers, strings);
    }

    private static void alphaTest(Tier[] tiers, int start, int stop, int direction)
    {
        for (int i = start; i != stop; i += direction)
        {
            String letter = ALPHABET[i].substring(0, 1);
            insertTest(tiers, letter);
        }
    
        for (int i = start; i != stop; i += direction)
        {
            assertTrue(contains(tiers, ALPHABET[i].substring(0, 1)));
        }
    }
    
    private static void containsTest(Tier[] tiers, String[] strings)
    {
        for (int i = 0; i < strings.length; i++)
        {
            assertTrue(contains(tiers, strings[i]));
        }
    }
    
    private static boolean hasSlot(Object[] slots) 
    {
        return slots[slots.length - 1] == null;
    }

    private static void insert(String[] strings, String string)
    {
        if (!hasSlot(strings))
        {
            throw new IllegalArgumentException();
        }
        
        int i;
        for (i = 0; strings[i] != null && strings[i].compareTo(string) < 0; i++)
        {
            // No operation.
        }
        
        System.arraycopy(strings, i, strings, i + 1, strings.length - (i + 1));
        strings[i] = string;
    }
    
    private final static int LEFT = 0;

    private final static int RIGHT = 1;
    
    private static String[][] split(String[] strings, int at)
    {
        String[] left = strings;
        String[] right = newStrings();
        System.arraycopy(strings, at, right, 0, strings.length - at);
        System.arraycopy(strings, 0, left, at, strings.length - at);
        Arrays.fill(left, at, strings.length, null);
        return new String[][] { left, right };
    }
    
    private static void insert(Tier[] tiers, int at, Tier tier)
    {
        System.arraycopy(tiers, at, tiers, at + 1, tiers.length - (at + 1));
        tiers[at] = tier;
    }

    private static void insert(Tier[] tiers, String string)
    {
        int i = 0;
        int index = 0;
        for (;;)
        {
            if (i + 1 == tiers.length || tiers[i + 1] == null)
            {
                break;
            }
            if (tiers[i + 1].ch > string.charAt(tiers[i + 1].index))
            {
                break;
            }
            i++;
        }
        if (i == tiers.length)
        {
            throw new IllegalStateException();
        }
        
        Tier tier = tiers[i];
        if (tier.children instanceof String[])
        {
            String[] strings = (String[]) tier.children;
            if (hasSlot(strings))
            {
                insert(strings, string);
            }
            else
            {
                split(tiers, i, index);
                insert(tiers, string);
            }
        }
        else
        {
            insert((Tier[]) tier.children, string);
        }
    }
    
    private static void split(Tier[] tiers, int i, int index)
    {
        assert i < tiers.length;
        assert tiers[i] != null;

        Tier tier = tiers[i];
        
        assert tier.children instanceof String[];
        
        String[] strings = (String[]) tier.children;
        
        int j = strings.length / 2;
        char partition = strings[j].charAt(index);
        for (;;)
        {
            if (j == 0)
            {
                break;
            }
            String s = strings[j - 1];
            if (index >= s.length())
            {
                throw new UnsupportedOperationException();
            }
            if (partition != s.charAt(index))
            {
                break;
            }
            j--;
        }

        if (j == 0)
        {
            for (;;)
            {
                j++;
                if (j == strings.length)
                {
                    break;
                }
                if (partition != strings[j].charAt(index))
                {
                    partition = strings[j].charAt(index);
                    break;
                }
            }
        }
        
        if (j == strings.length)
        {
            char ch = strings[0].charAt(index);
            if (tier.ch == Character.MIN_VALUE)
            {
                assert i == 0;
                assert index == 0;
                tiers[0] = new Tier(Character.MIN_VALUE, 0, newStrings());
                insert(tiers, 1, new Tier(ch, 0, tier.children));
            }
            else if (tier.ch != ch)
            {
                throw new IllegalStateException();
            }
            else
            {
                split(tiers, i, index + 1);
            }
        }
        else if (tiers[tiers.length - 1] == null)
        {
            String[][] split = split(strings, j);
            insert(tiers, i + 1, new Tier(partition, index, split[RIGHT]));
            tiers[i] = new Tier(tier.ch, tier.index, split[LEFT]);
        }
        else
        {
            String[][] split = split(strings, j);
            Tier[] subTiers = new Tier[TIERS_LENGTH];
            subTiers[0] = new Tier(tier.ch, tier.index, split[LEFT]);
            subTiers[1] = new Tier(partition, index, split[RIGHT]);
            tiers[i] = new Tier(tier.ch, tier.index, subTiers);
        }
    }

    private static boolean contains(String[] strings, String string)
    {
        for (int i = 0; i < strings.length && strings[i] != null; i++)
        {
            if (strings[i].equals(string))
            {
                return true;
            }
        }
        return false;
    }
 
    private static boolean contains(Tier[] tiers, String string)
    {
        int i = 0;
        for (;;)
        {
            if (i + 1 == tiers.length || tiers[i + 1] == null)
            {
                break;
            }
            if (tiers[i + 1].ch > string.charAt(tiers[i + 1].index))
            {
                break;
            }
            i++;
        }
        if (tiers[i].children instanceof String[])
        {
            return contains((String[]) tiers[i].children, string);
        }
        return contains((Tier[]) tiers[i].children, string);
    }
    
    private final static int TIERS_LENGTH = 3;
    
    private final static int STRINGS_LENGTH = 3;
    
    private static String[] newStrings()
    {
        return new String[STRINGS_LENGTH];
    }

    private static Tier[] newFirstTier()
    {
        Tier[] tiers = new Tier[TIERS_LENGTH];
        tiers[0] = new Tier(Character.MIN_VALUE, 0, newStrings());
        return tiers;
    }
    
    public void testInsertFirstString()
    {
       Tier[] tiers = newFirstTier();
       insert(tiers, "bad");
       assertTrue(contains(tiers, "bad"));
       assertStructure(tiers);
    }


    public void testInsertTwoStringsInOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bad");
        insertTest(tiers, "bid");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
        assertStructure(tiers);
    }

    public void testInsertTwoStringsOutOfOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bid");
        insertTest(tiers, "bad");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
        assertStructure(tiers);
    }

    public void testInsertThreeStringsInOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bad");
        insertTest(tiers, "bed");
        insertTest(tiers, "bid");
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bed"));
        assertTrue(contains(tiers, "bad"));
        assertStructure(tiers);
    }

    public void testInsertThreeStringsOutOfOrder()
    {
        Tier[] tiers = newFirstTier();
        insertTest(tiers, "bid");
        insertTest(tiers, "bad");
        insertTest(tiers, "bed");
        assertTrue(contains(tiers, "bed"));
        assertTrue(contains(tiers, "bid"));
        assertTrue(contains(tiers, "bad"));
        assertStructure(tiers);
    }

    public void testInsertSplitPage()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "act", "bad", "cat" });
        insertTest(tiers, "car");
        assertStructure(tiers);
    }

    public void testInsertSplitPageBeginningWithSought()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "bid", "bad", "cat" });
        insertTest(tiers, "bed");
        assertStructure(tiers);
    }

    public void testInsertOntoFirstPageAfterSplit()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "act", "bad", "cat" });
        insertTest(tiers, "add");
        assertStructure(tiers);
    }
    
    public void testInsertLeftSplitOnSecondCharacter()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "mid", "mud", "med" });
        insertTest(tiers, "mad");
        assertStructure(tiers);
    }

    public void testInsertRightSplitOnSecondCharacter()
    {
        Tier[] tiers = newFirstTier();

        insertTest(tiers, new String[] { "mid", "mad", "med" });
        insertTest(tiers, "mud");
        assertStructure(tiers);
    }

    // XXX Probably can constantly unallocate the first page, maybe it doesn't exist?
    public void testSplitTier()
    {
        Tier[] tiers = newFirstTier();

        alphaTest(tiers, 0, 9, 1);
        assertStructure(tiers);
    }

    public void testAlphabet()
    {
        Tier[] tiers = newFirstTier();

        alphaTest(tiers, 0, 25, 1);
        assertStructure(tiers);
    }
    
    public void testIssueSplitSearchingRightForPartition()
    {
        Tier[] tiers = newFirstTier();
        
        insert(tiers, "n");
        insert(tiers, "m");
        insert(tiers, "m");
        insert(tiers, "n");
        
        assertStructure(tiers);
    }

    public void testAlphaScattered()
    {
        Tier[] tiers = newFirstTier();
        
        for (int i = 13; i != -1; i--)
        {
            String left = ALPHABET[i].substring(0, 1);
            insertTest(tiers, left);
            String right = ALPHABET[25 - i].substring(0, 1);
            insertTest(tiers, right);
        }
        
        assertStructure(tiers);

        alphaSearch(tiers, 0, 26, 1);
    }
    
    private static void alphaSearch(Tier[] tiers, int from, int to, int advance)
    {
        for (int i = from; i != to; i += advance)
        {
            String letter = ALPHABET[i].substring(0, 1);
            assertTrue(contains(tiers, letter));
        }
    }
    
    private static void assertCorrectIndex(LinkedList listOfCollationUnits,
                                           int index)
    {
        Iterator units = listOfCollationUnits.iterator();
        while (units.hasNext())
        {
            CollationUnit unit = (CollationUnit) units.next();
            assertTrue(unit.index <= index);
        }
    }

    private static boolean isLessThan(LinkedList listOfCollationUnits,
                                      String string)
    {
        Iterator units = listOfCollationUnits.iterator();
        while (units.hasNext())
        {
            CollationUnit unit = (CollationUnit) units.next();
            if
            (
                string.length() <= unit.index
                || string.charAt(unit.index) < unit.ch
            )
            {
                return true;
            }
        }
        return false;
    }

    private static void assertLessThan(LinkedList listOfCollationUnits,
                                       String string)
    {
        assertTrue(isLessThan(listOfCollationUnits, string));
    }

    private static void assertGreaterOrEquals(LinkedList listOfCollationUnits,
                                              String string)
    {
        Iterator units = listOfCollationUnits.iterator();
        while (units.hasNext())
        {
            CollationUnit unit = (CollationUnit) units.next();
            assertTrue(unit.index < string.length());
            assertTrue(string.charAt(unit.index) >= unit.ch);
        }
    }

    private static void assertSorted(Tier[] tiers, String[] previous)
    {
        int i = 0;
        for (i = 0; i < tiers.length && tiers[i] != null; i++)
        {
            if (i != 0)
            {
                assertTrue(tiers[i].ch != Character.MAX_VALUE);
            }
            assertSorted(tiers[i], previous);
        }
        for (; i < tiers.length; i++)
        {
            assertNull(tiers[i]);
        }
    }

    private static void assertSorted(Tier tier, String[] previous)
    {
        if (tier.children instanceof Tier[])
        {
            assertSorted((Tier[]) tier.children, previous);
        }
        else if (tier.children instanceof String[])
        {
            String[] strings = (String[]) tier.children;
            int i;
            for (i = 0; i < strings.length && strings[i] != null; i++)
            {
                if (previous[0] != null)
                {
                    assertTrue(previous[0].compareTo(strings[i]) <= 0);
                }
                previous[0] = strings[i];
            }
            for (; i < strings.length; i++)
            {
                assertTrue(strings[i] == null);
            }
        }
        else
        {
            fail("Children are of wrong type.");
        }
    }

    private static void assertStructure(Tier tier,
                                        LinkedList listOfCollationUnits,
                                        String[] previous)
    {
        assertCorrectIndex(listOfCollationUnits, tier.index);
        listOfCollationUnits.addLast(new CollationUnit(tier.ch, tier.index));
        if (previous[0] != null)
        {
            assertLessThan(listOfCollationUnits, previous[0]);
        }
        if (tier.children instanceof Tier[])
        {
            assertStructure((Tier[]) tier.children, listOfCollationUnits, previous);
        }
        else if (tier.children instanceof String[])
        {
            String[] strings = (String[]) tier.children;
            int i;
            for (i = 0; i < strings.length && strings[i] != null; i++)
            {
                assertGreaterOrEquals(listOfCollationUnits, strings[i]);
                previous[0] = strings[i];
            }
            for (; i < strings.length; i++)
            {
                assertTrue(strings[i] == null);
            }
        }
        else
        {
            fail("Children are of wrong type.");
        }
        listOfCollationUnits.removeLast();
    }
    
    private static void assertStructure(Tier[] tiers)
    {
        assertSorted(tiers, new String[1]);
        assertStructure(tiers, new LinkedList(), new String[1]);
    }

    private static void assertStructure(Tier[] tiers,
                                        LinkedList listOfCollationUnits,
                                        String[] previous)
    {
        int i = 0;
        for (i = 0; i < tiers.length && tiers[i] != null; i++)
        {
            if (i != 0)
            {
                assertTrue(tiers[i].ch != Character.MAX_VALUE);
            }
            assertStructure(tiers[i], listOfCollationUnits, previous);
        }
        for (; i < tiers.length; i++)
        {
            assertNull(tiers[i]);
        }
    }
    
    private static class CollationUnit
    {
        private final char ch;
        
        private final int index;
        
        public CollationUnit(char ch, int index)
        {
            this.ch = ch;
            this.index = index;
        }
        
        public boolean equals(Object object)
        {
            if (object instanceof CollationUnit)
            {
                CollationUnit unit = (CollationUnit) object;
                return unit.ch == ch && unit.index == index;
            }
            return false;
        }
        
        public int hashCode()
        {
            int hashCode = 7;
            hashCode = hashCode * 37 + ch;
            hashCode = hashCode * 37 + index;
            return hashCode;
        }
    }
    
    private static void runViewer()
    {
//        Tier[] tiers = newFirstTier();
//
//        alphaTest(tiers, 0, 25, 1);
       Tier[] tiers = newFirstTier();
        
        for (int i = 13; i != -1; i--)
        {
            String left = ALPHABET[i].substring(0, 1);
            insertTest(tiers, left);
            String right = ALPHABET[25 - i].substring(0, 1);
            insertTest(tiers, right);
        }

        JFrame.setDefaultLookAndFeelDecorated(true);
        
        JFrame frame = new JFrame("Strata-Vision");
        
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        
        DefaultMutableTreeNode top = new DefaultMutableTreeNode("Strata");
        top.setAllowsChildren(true);
        
        TreeModel strataModel = new StrataTreeModel(new Tier(Character.MIN_VALUE, 0, tiers));
        
        JTree tree = new JTree(strataModel);
        tree.collapsePath(tree.getPathForRow(0));
        JScrollPane treeScroll = new JScrollPane(tree);

        Dimension minimumSize = new Dimension(200, 400);
        treeScroll.setMinimumSize(minimumSize);
        
        JPanel right = new JPanel();
        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,
                                              treeScroll,
                                              right);

        splitPane.setOneTouchExpandable(true);
        splitPane.setLastDividerLocation(100);
        
        frame.getContentPane().add(splitPane);
        
        frame.pack();
        frame.setVisible(true);
    }
    
//    private final static class StrataTreeExpandListeners
//    implements TreeExpansionListener
//    {
//        public void treeCollapsed(TreeExpansionEvent event)
//        {
//        }
//        
//        public void treeExpanded(TreeExpansionEvent event)
//        {
//            System.out.println(event.getSource());
//        }
//    }
    
    /** Because string internment is defeating equality. */
    private final static class StrataTreeModelLeaf
    {
        private final String string;
        
        private final int index;

        public StrataTreeModelLeaf(String string, int index)
        {
            this.string = string;
            this.index = index;
        }
        
        public int getIndex()
        {
            return index;
        }
        
        public String toString()
        {
            return string;
        }
    }
    

    private final static class StrataTreeModel
    implements TreeModel
    {
        private final Tier tier;
        
        private final EventListenerList listOfEventListeners;
        
        private final WeakHashMap mapOfUserData;
        
        public StrataTreeModel(Tier tier)
        {
            this.tier = tier;
            this.listOfEventListeners = new EventListenerList();
            this.mapOfUserData = new WeakHashMap();
        }
        
        public void valueForPathChanged(TreePath path, Object newValue)
        {
            Object object = path.getLastPathComponent();
            mapOfUserData.put(object, newValue);
            Object[] listeners = listOfEventListeners.getListenerList();
            for (int i = listeners.length - 2; i > -1; i -= 2)
            {
                TreeModelListener listener;
                if (TreeModelListener.class == listeners[i])
                {
                    listener = (TreeModelListener) listeners[i + 1];
                    TreeModelEvent event = new TreeModelEvent(this, path);
                    listener.treeNodesChanged(event);
                }
            }
        }

        public void addTreeModelListener(TreeModelListener l)
        {
            listOfEventListeners.add(TreeModelListener.class, l);
        }
        
        public void removeTreeModelListener(TreeModelListener l)
        {
            listOfEventListeners.remove(TreeModelListener.class, l);
        }
        
        public Object getChild(Object parent, int index)
        {
            Tier tier = (Tier) parent;
            if (tier.children instanceof String[])
            {
                String string = ((String[]) tier.children)[index];
                return new StrataTreeModelLeaf(string, index);
            }
            return ((Tier[]) tier.children)[index];
        }
        
        public int getChildCount(Object parent)
        {
            Tier tier = (Tier) parent;
            int i;
            Object[] children = tier.children;
            for (i = 0; i < children.length && children[i] != null; i++)
            {
            }
            return i;
        }
        
        public int getIndexOfChild(Object parent, Object child)
        {
            if (parent == null || child == null)
            {
                return -1;
            }
            Tier tier = (Tier) parent;
            if (tier.children instanceof String[])
            {
                return ((StrataTreeModelLeaf) child).getIndex();
            }
            for (int i = 0; i < tier.children.length; i++)
            {
                if (tier.children[i] == child)
                {
                    return i;
                }
            }
            return -1;
        }
        
        public Object getRoot()
        {
            return tier;
        }
        
        public boolean isLeaf(Object node)
        {
            return node instanceof StrataTreeModelLeaf;
        }
    }

    public static void main(String[] args)
    {
        SwingUtilities.invokeLater(new Runnable()
        {
            public void run()
            {
                runViewer();
            }
        });
    }
}

/* vim: set et sw=4 ts=4 ai tw=72: */
