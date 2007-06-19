/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.event.EventListenerList;
import javax.swing.event.TreeModelEvent;
import javax.swing.event.TreeModelListener;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

public class Viewer
{
    private final static class TreeNode
    {
        public final Strata.Tier tier;

        public final String text;

        public TreeNode(Strata.Tier tier, String text)
        {
            this.tier = tier;
            this.text = text;
        }

        public String toString()
        {
            return text;
        }
    }

    private final static class TreeModelStorage
    implements TreeModel, Strata.Storage
    {
        private final static long serialVersionUID = 20070613L;

        private final Strata.Storage storage = new ArrayListStorage();

        private final EventListenerList listOfListeners = new EventListenerList();

        private Strata.Tier root;

        public void addTreeModelListener(TreeModelListener listener)
        {
            listOfListeners.add(TreeModelListener.class, listener);
        }

        public void removeTreeModelListener(TreeModelListener listener)
        {
            listOfListeners.remove(TreeModelListener.class, listener);
        }

        public Object getChild(Object parent, int index)
        {
            TreeNode node = (TreeNode) parent;
            if (node.tier instanceof Strata.InnerTier)
            {
                Strata.InnerTier inner = (Strata.InnerTier) node.tier;
                Strata.Branch branch = (Strata.Branch) inner.get(index);
                Strata.Tier tier = inner.getTier(null, branch.getRightKey());
                Object pivot = branch.getPivot();
                return new TreeNode(tier, pivot == null ? "<" : pivot.toString());
            }
            Strata.LeafTier leaf = (Strata.LeafTier) node.tier;
            if (index == leaf.getSize())
            {
                return "<";
            }
            return leaf.get(index);
        }

        public int getChildCount(Object object)
        {
            TreeNode node = (TreeNode) object;
            return node.tier.getSize() + 1;
        }

        public int getIndexOfChild(Object parent, Object child)
        {
            Strata.InnerTier inner = (Strata.InnerTier) ((TreeNode) parent).tier;
            Strata.Tier lower = (Strata.Tier) ((TreeNode) child).tier;
            return inner.getIndexOfTier(lower.getKey());
        }

        public Object getRoot()
        {
            return new TreeNode(root, null);
        }

        public boolean isLeaf(Object object)
        {
            return !(object instanceof TreeNode);
        }

        public void valueForPathChanged(TreePath path, Object object)
        {
            throw new UnsupportedOperationException();
        }

        public void fire()
        {
            TreeModelListener[] listeners = (TreeModelListener[]) listOfListeners.getListeners(TreeModelListener.class);
            for (int i = 0; i < listeners.length; i++)
            {
                listeners[i].treeStructureChanged(new TreeModelEvent(this, new Object[] { root }));
            }
        }

        public Strata.InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
        {
            Strata.InnerTier inner = storage.newInnerTier(structure, txn, typeOfChildren);
            if (root == null)
            {
                root = inner;
            }
            return inner;
        }

        public Strata.LeafTier newLeafTier(Strata.Structure structure, Object txn)
        {
            return storage.newLeafTier(structure, txn);
        }

        public Strata.InnerTier getInnerTier(Strata.Structure structure, Object txn, Object key)
        {
            Strata.InnerTier inner = storage.getInnerTier(structure, txn, key);
            if (root == null)
            {
                root = inner;
            }
            return inner;
        }

        public Strata.LeafTier getLeafTier(Strata.Structure structure, Object txn, Object key)
        {
            return storage.getLeafTier(structure, txn, key);
        }

        public Object getKey(Strata.Tier tier)
        {
            return storage.getKey(tier);
        }

        public boolean isKeyNull(Object object)
        {
            return storage.isKeyNull(object);
        }

        public void write(Strata.Structure structure, Object txn, Strata.InnerTier inner)
        {
            storage.write(structure, txn, inner);
        }

        public void write(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
        {
            storage.write(structure, txn, leaf);
        }

        public void free(Strata.Structure structure, Object txn, Strata.InnerTier inner)
        {
            storage.free(structure, txn, inner);
        }

        public void free(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
        {
            storage.free(structure, txn, leaf);
        }

        public void commit(Object txn)
        {
            storage.commit(txn);
        }
    }

    private final static class AddNumber
    implements ActionListener
    {
        private final JTextField entry;

        private final JTree tree;

        private final Strata strata;

        public AddNumber(JTextField entry, JTree tree, Strata strata)
        {
            this.entry = entry;
            this.tree = tree;
            this.strata = strata;
        }

        public void actionPerformed(ActionEvent event)
        {
            int number;
            try
            {
                number = Integer.parseInt(entry.getText());
            }
            catch (NumberFormatException e)
            {
                return;
            }

            strata.query(null).insert(new Integer(number));

            TreeModelStorage treeModel = (TreeModelStorage) tree.getModel();
            treeModel.fire();

            int i = 0;
            while (i < tree.getRowCount())
            {
                tree.expandRow(i);
                i++;
            }

            entry.setText("");
        }
    }

    // If expand is true, expands all nodes in the tree.
    // Otherwise, collapses all nodes in the tree.
    public static void expandAll(JTree tree, boolean expand)
    {
        TreeModel model = tree.getModel();
        Object root = model.getRoot();

        // Traverse tree from root
        expandAll(tree, new TreePath(root), expand);
    }

    private static void expandAll(JTree tree, TreePath parent, boolean expand)
    {
        TreeModel model = tree.getModel();

        // Traverse children
        Object node = parent.getLastPathComponent();
        if (!model.isLeaf(node))
        {
            int childCount = model.getChildCount(node);
            for (int i = 0; i < childCount; i++)
            {
                Object n = model.getChild(node, i);
                TreePath path = parent.pathByAddingChild(n);
                expandAll(tree, path, expand);
            }
        }

        // Expansion or collapse must be done bottom-up
        if (expand)
        {
            tree.expandPath(parent);
        }
        else
        {
            tree.collapsePath(parent);
        }
    }

    public final static void main(String[] args)
    {
        JFrame frame = new JFrame("Hello, World");

        frame.setLayout(new BorderLayout());

        TreeModelStorage storage = new TreeModelStorage();

        Strata.Creator creator = new Strata.Creator();

        creator.setSize(3);
        creator.setStorage(storage);

        Strata strata = creator.create(null);

        JPanel panel = new JPanel();

        JTextField entry = new JTextField(10);
        JButton add = new JButton("Add");

        panel.add(entry);
        panel.add(add);

        frame.add(panel, BorderLayout.NORTH);

        JTree tree = new JTree(storage);
        tree.setRootVisible(false);
        frame.add(tree, BorderLayout.CENTER);

        add.addActionListener(new AddNumber(entry, tree, strata));

        frame.pack();
        frame.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        frame.setVisible(true);

        // expandAll(tree, true);
        int i = 0;
        while (i < tree.getRowCount())
        {
            tree.expandRow(i);
            i++;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */