/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

import java.awt.BorderLayout;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.WindowConstants;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.event.EventListenerList;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

import com.goodworkalan.stash.Stash;

/**
 * A viewer to create unit test for Strata.
 *
 * @author Alan Gutierrez
 */
public class Viewer {
    /**
     * A node in the tree viewer.
     * 
     * @author Alan Gutierrez
     */
    private final static class TreeNode {
        /** The tier. */
        public final CharacterTier tier;

        /** The character. */
        public final Character ch;
        
        /** Whether or not the tier is a leaf tier. */
        public final boolean leaf;

        /**
         * Create a new tree node. 
         * @param tier The tier. 
         * @param ch The character. 
         * @param leaf Whether or not the tier is a leaf tier.
         */
        public TreeNode(CharacterTier tier, Character ch, boolean leaf) {
            this.tier = tier;
            this.ch = ch;
            this.leaf = leaf;
        }

        public String toString() {
            return ch.toString();
        }
    }

    /**
     * A JTree tree model to display a tree composed of
     * <code>CharacterTier</code> instances. This tree model navigates the tree
     * of <code>CharacterTier</code> instances separately from the
     * <code>Strata</code> that creates them. This is possible since
     * <code>Strata</code> maintains the same root tier for the life of the
     * <code>Strata</code> tree and because character tier uses a Java reference
     * as the addressing model, that is no real addressing model, just holding
     * references to the next tier instead of file positions or the like.
     * 
     * @author Alan Gutierrez
     */
    private final static class CharacterTierTreeModel implements TreeModel {
        /** The serial version id. */
        private final static long serialVersionUID = 20070613L;

        /** The root tier. */
        private CharacterTier root;

        /** The list of tree mdoel event listeners. */
        private final EventListenerList listeners = new EventListenerList();

        /**
         * Create a character tier tree with the given root character tier.
         * 
         * @param root
         *            The root character tier.
         */
        public CharacterTierTreeModel(CharacterTier root) {
            this.root = root;
        }

        /**
         * Add an event listener. The JTree will add itself and listen for
         * change events.
         * 
         * @param listener
         *            The listener to add.
         */
        public void addTreeModelListener(TreeModelListener listener) {
            listeners.add(TreeModelListener.class, listener);
        }

        /**
         * Remove an event listener.
         * 
         * @param listner
         *            The listener to remove.
         */
        public void removeTreeModelListener(TreeModelListener listener) {
            listeners.remove(TreeModelListener.class, listener);
        }

        /**
         * Get the child object at the given index from the the given parent
         * object.
         * 
         * @param parent
         *            The parent object.
         * @param index
         *            The index.
         */
        public Object getChild(Object parent, int index) {
            TreeNode node = (TreeNode) parent;

            if (!node.leaf) {
                CharacterTier inner = node.tier.getChildAddress(index);
                Character ch = node.tier.getRecord(index);
                return new TreeNode(inner, ch == null ? '<' : ch, node.tier.isChildLeaf());
            }

            CharacterTier leaf = node.tier;
            int size = 4; // FIXME Hard coded.
            if (index == size) {
                CharacterTier next = leaf.getNext();
                if (next != null && next.getSize() != 0  && next.getRecord(0).equals(leaf.getRecord(0))) {
                    return next.getRecord(0) + " [" + index + "]";
                }
                return "<";
            } else if (index > size) {
                Object object = leaf.getRecord(0);
                int offset = index;
                while (offset >= size) {
                    offset -= size;
                    leaf = leaf.getNext();
                }
                if (leaf == null || !leaf.getRecord(0).equals(object) || leaf.getSize() == offset) {
                    return "<";
                }
                return leaf.getRecord(0) + " [" + index + "]";
            }

            if (index == leaf.getSize()) {
                return "<";
            }

            Object object = leaf.getRecord(index);
            int offset = index;
            while (offset != 0 && object.equals(leaf.getRecord(offset - 1))) {
                offset--;
            }

            if (offset == index) {
                if (index + 1 != leaf.getSize() && object.equals(leaf.getRecord(index + 1))) {
                    return object + " [0]";
                }

                return object;
            }

            return leaf.getRecord(index) + " [" + (index - offset) + "]";
        }

        private boolean linkedLeaves(TreeNode node) {
            if (!node.leaf) {
                return false;
            }
            CharacterTier leaf = node.tier;
            int size = 4;
            if (leaf.getSize() == size && leaf.getRecord(0).equals(leaf.getRecord(size - 1))) {
                return true;
            }
            return false;
        }
        
        public int getChildCount(Object object) {
            TreeNode node = (TreeNode) object;
            if (linkedLeaves(node)) {
                CharacterTier leaf = node.tier;
                CharacterTier previous;
                int count = 0;
                do {
                    count += leaf.getSize();
                    previous = leaf;
                    leaf = leaf.getNext();
                } while (leaf != null && previous.getRecord(0).equals(leaf.getRecord(0)));

                return count;
            }
            return node.tier.getSize();
        }

        public int getIndexOfChild(Object parent, Object child) {
            throw new UnsupportedOperationException();
        }

        public Object getRoot() {
            return new TreeNode(root, '*', false);
        }

        public boolean isLeaf(Object object) {
            return !(object instanceof TreeNode);
        }

        public void valueForPathChanged(TreePath path, Object object) {
            throw new UnsupportedOperationException();
        }

        public void fire() {
            TreeModelListener[] all =  listeners.getListeners(TreeModelListener.class);
            for (int i = 0; i < all.length; i++) {
                all[i].treeStructureChanged(new TreeModelEvent(this, new Object[] { new TreeNode(root, null, true) }));
            }
        }
    }

    private final static class CopyTest
    extends AbstractAction {
        private static final long serialVersionUID = 20070620L;

        private final CharacterTier root;

        public CopyTest(CharacterTier root) {
            this.root = root;
            putValue(Action.NAME, "Copy Test To Clipboard");
        }

        public void actionPerformed(ActionEvent arg0) {
            Clipboard system = Toolkit.getDefaultToolkit().getSystemClipboard();
            StringBuilder builder = new StringBuilder();
            controlValue(root, false, 0, builder);
            StringSelection selection = new StringSelection(builder.toString());
            system.setContents(selection, selection);
        }
    }
    
    static StringBuilder controlValue(CharacterTier root, boolean leaf, int depth, StringBuilder string) {
        for (int i = 0, stop = root.getSize(); i < stop; i++) {
            for (int j = 0; j < (depth * 2); j++) {
                string.append(' ');
            }
            string.append(root.getRecord(i)).append('\n');
            if (!leaf) {
                controlValue(root.getChildAddress(i), root.isChildLeaf(), depth + 1, string);
            }
        }
        return string;
    }

    public final static void main(String[] args) {
        JFrame frame = new JFrame("Hello, World");

        frame.setLayout(new BorderLayout());


        JPanel panel = new JPanel();

        frame.add(panel, BorderLayout.NORTH);

        Schema<Character> schema = new Schema<Character>();
        schema.setInnerCapacity(4);
        schema.setLeafCapacity(4);
        schema.setComparableFactory(new CastComparableFactory<Character>());
        CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
        final Strata<Character> strata =  schema.open(new Stash(), address, new CharacterTierStorage());

        final CharacterTierTreeModel model = new CharacterTierTreeModel(address);
        final JTree tree = new JTree(model);
        tree.setRootVisible(false);
        frame.add(new JScrollPane(tree), BorderLayout.CENTER);
        final JTextField editor = new JTextField(20);
        editor.getDocument().addDocumentListener(new DocumentListener() {
            public void removeUpdate(DocumentEvent e) {
            }
            
            public void insertUpdate(DocumentEvent e) {
                String text = editor.getText();
                if (text.startsWith(operations.toString())) {
                    for (int i = operations.length(), stop = text.length(); i < stop; i++) {
                        char ch = text.charAt(i);
                        if (ch == '/') {
                            adding = !adding;
                        } else {
                            Query<Character> query = strata.query();
                            if (adding) {
                                query.add(ch);
                            } else {
                                query.remove(ch);
                            }
                            query.destroy();
                        }
                        operations.append(ch);
                    } 
                } else {
                    // FIXME Clear Strata and start over.
                }

                model.fire();

                int i = 0;
                while (i < tree.getRowCount()) {
                    tree.expandRow(i);
                    i++;
                }
            }
            
            boolean adding = true;
            StringBuilder operations = new StringBuilder();

            public void changedUpdate(DocumentEvent e) {
                
            }
        });
        panel.add(editor);

        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("File");
        menuBar.add(menu);

        menu.addSeparator();

        JMenuItem copyTests = new JMenuItem(new CopyTest(address));
        menu.add(copyTests);

        frame.setJMenuBar(menuBar);

        frame.pack();
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.setVisible(true);

        // expandAll(tree, true);
        int i = 0;
        while (i < tree.getRowCount()) {
            tree.expandRow(i);
            i++;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */