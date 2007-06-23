/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.KeyStroke;
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
            int size = leaf.getStructure().getSize();
            if (index == size)
            {
                Strata.LeafTier next = storage.getLeafTier(leaf.getStructure(), null, leaf.getNextLeafKey());
                if (next != null && next.get(0).equals(leaf.get(0)))
                {
                    return next.get(0) + " [" + index + "]";
                }
                return "<";
            }
            else if (index > size)
            {
                Object object = leaf.get(0);
                int offset = index;
                while (offset >= size)
                {
                    offset -= size;
                    leaf = storage.getLeafTier(leaf.getStructure(), null, leaf.getNextLeafKey());
                }
                if (leaf == null || !leaf.get(0).equals(object) || leaf.getSize() == offset)
                {
                    return "<";
                }
                return leaf.get(0) + " [" + index + "]";
            }
            
            if (index == leaf.getSize())
            {
                return "<";
            }

            Object object = leaf.get(index);
            int offset = index;
            while (offset != 0 && object.equals(leaf.get(offset - 1)))
            {
                offset--;
            }

            if (offset == index)
            {
                if (index + 1 != leaf.getSize() && object.equals(leaf.get(index + 1)))
                {
                    return object + " [0]";
                }

                return object;
            }

            return leaf.get(index) + " [" + (index-offset) + "]";
        }

        private boolean linkedLeaves(Strata.Tier tier)
        {
            if (tier instanceof Strata.InnerTier)
            {
                return false;
            }
            Strata.LeafTier leaf = (Strata.LeafTier) tier;
            int size = leaf.getStructure().getSize();
            if (leaf.getSize() == size && leaf.get(0).equals(leaf.get(size - 1)))
            {
                return true;
            }
            return false;
        }

        public int getChildCount(Object object)
        {
            TreeNode node = (TreeNode) object;
            if (linkedLeaves(node.tier))
            {
                Strata.LeafTier leaf = (Strata.LeafTier) node.tier;
                Strata.LeafTier previous;
                int count = 0;
                do
                {
                    count += leaf.getSize();
                    previous = leaf;
                    leaf = storage.getLeafTier(leaf.getStructure(), null, leaf.getNextLeafKey());
                }
                while (leaf != null && previous.get(0).equals(leaf.get(0)));
                
                return count + 1;
            }
            return node.tier.getSize() + 1;
        }

        public int getIndexOfChild(Object parent, Object child)
        {
            throw new UnsupportedOperationException();
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
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070620L;

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

            TreeModelStorage model = (TreeModelStorage) tree.getModel();
            model.fire();

            int i = 0;
            while (i < tree.getRowCount())
            {
                tree.expandRow(i);
                i++;
            }

            entry.setText("");
        }
    }

    private final static class RemoveNumber
    extends MouseAdapter
    {
        private final JTree tree;

        private final Strata strata;

        public RemoveNumber(JTree tree, Strata strata)
        {
            this.tree = tree;
            this.strata = strata;
        }

        public void mousePressed(MouseEvent event)
        {
            int selRow = tree.getRowForLocation(event.getX(), event.getY());
            TreePath selPath = tree.getPathForLocation(event.getX(), event.getY());
            if (selRow != -1 && event.getClickCount() == 2)
            {
                TreeModelStorage model = (TreeModelStorage) tree.getModel();
                Object object = selPath.getLastPathComponent();
                if (model.isLeaf(object) && !"<".equals(object))
                {
                    String value = object.toString();
                    int space = value.indexOf(' ');
                    if (space != -1)
                    {
                        value = value.substring(0, space);
                    }

                    int number;
                    try
                    {
                        number = Integer.parseInt(value);
                    }
                    catch (NumberFormatException e)
                    {
                        return;
                    }
                    strata.query(null).remove(new Integer(number));

                    model.fire();

                    int i = 0;
                    while (i < tree.getRowCount())
                    {
                        tree.expandRow(i);
                        i++;
                    }
                }
            }
        }
    };

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

        creator.setSize(2);
        creator.setStorage(storage);

        Strata strata = creator.create(null);

        JPanel panel = new JPanel();

        JTextField entry = new JTextField(10);
        JButton add = new JButton("Add");

        panel.add(entry);
        panel.add(add);

        frame.add(panel, BorderLayout.NORTH);

        JTree tree = new JTree(storage);
        tree.addMouseListener(new RemoveNumber(tree, strata));
        tree.setRootVisible(false);
        frame.add(tree, BorderLayout.CENTER);

        AddNumber addNumber = new AddNumber(entry, tree, strata);
        entry.getActionMap().put("addNumber", addNumber);
        entry.getInputMap().put(KeyStroke.getKeyStroke("ENTER"), "addNumber");
        add.addActionListener(addNumber);

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