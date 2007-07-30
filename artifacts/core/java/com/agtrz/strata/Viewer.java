/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Toolkit;
import java.awt.datatransfer.Clipboard;
import java.awt.datatransfer.StringSelection;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.KeyStroke;
import javax.swing.SpinnerNumberModel;
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
                if (next != null && next.getSize() != 0 && next.get(0).equals(leaf.get(0)))
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

            return leaf.get(index) + " [" + (index - offset) + "]";
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

        public void reset()
        {
            root = null;
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

    private interface Operation
    {
        public void operate(int number);
    }

    private final static class Insert
    implements Operation
    {
        private final StrataCozy strataCozy;

        private final StringBuffer actions;

        private final StringBuffer tests;

        public Insert(StrataCozy strataCozy, StringBuffer actions, StringBuffer tests)
        {
            this.strataCozy = strataCozy;
            this.actions = actions;
            this.tests = tests;
        }

        public void operate(int number)
        {
            actions.append("A" + number + "\n");
            tests.append("query.insert(new Integer(" + number + "));\n");

            strataCozy.getStrata().query(null).insert(new Integer(number));
            strataCozy.getStrata().query(null).copacetic();
        }
    }

    private final static class Remove
    implements Operation
    {
        private final StrataCozy strataCozy;

        private final StringBuffer actions;

        private final StringBuffer tests;

        public Remove(StrataCozy strataCozy, StringBuffer actions, StringBuffer tests)
        {
            this.strataCozy = strataCozy;
            this.actions = actions;
            this.tests = tests;
        }

        public void operate(int number)
        {
            actions.append("D" + number + "\n");
            tests.append("query.remove(new Integer(" + number + "));\n");

            strataCozy.getStrata().query(null).remove(new Integer(number));
            // strata.query(null).copacetic();
        }
    }

    private final static class SaveFile
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070620L;

        private final Component parent;

        private final StringBuffer buffer;

        public SaveFile(Component parent, StringBuffer buffer)
        {
            this.parent = parent;
            this.buffer = buffer;
            putValue(Action.NAME, "Save");
        }

        public void actionPerformed(ActionEvent event)
        {
            JFileChooser chooser = new JFileChooser();
            int choice = chooser.showSaveDialog(null);
            if (choice == JFileChooser.APPROVE_OPTION)
            {
                File file = chooser.getSelectedFile();
                FileWriter writer;
                try
                {
                    writer = new FileWriter(file);
                    writer.write(buffer.toString());
                    writer.close();
                }
                catch (IOException e)
                {
                    JOptionPane.showMessageDialog(parent, "Cannot write file.");
                }
            }
        }
    }

    private final static class SetSize
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070729L;

        private final StrataCozy strataCozy;

        private final JTree tree;

        public SetSize(StrataCozy strataCozy, JTree tree)
        {
            this.strataCozy = strataCozy;
            this.tree = tree;
            putValue(Action.NAME, "Set Tier Size...");
        }

        public void actionPerformed(ActionEvent arg0)
        {
            SpinnerNumberModel number = new SpinnerNumberModel();
            number.setMinimum(new Integer(2));
            number.setValue(new Integer(strataCozy.getSize()));
            String[] options = new String[] { "OK", "Cancel" };
            int result = JOptionPane.showOptionDialog(null, new Object[] { "Set size of Strata tiers.", new JSpinner(number) }, "Set Tier Size", JOptionPane.OK_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
            if (result == 0)
            {
                strataCozy.setSize(((Integer) number.getValue()).intValue());
            }

            TreeModelStorage model = (TreeModelStorage) tree.getModel();
            model.fire();

            int i = 0;
            while (i < tree.getRowCount())
            {
                tree.expandRow(i);
                i++;
            }
        }
    }

    private final static class StrataCozy
    {
        private TreeModelStorage storage;

        private Strata strata;

        private int size;

        public StrataCozy(int size)
        {
            this.storage = new TreeModelStorage();
            setSize(size);
        }

        public void setSize(int size)
        {
            storage.reset();

            Strata.Creator creator = new Strata.Creator();

            creator.setSize(size);
            creator.setStorage(storage);

            this.size = size;
            this.strata = creator.create(null);
        }

        public void reset()
        {
            setSize(getSize());
        }

        public int getSize()
        {
            return size;
        }

        public Strata getStrata()
        {
            return strata;
        }

        public TreeModel getTreeModel()
        {
            return storage;
        }
    }

    private final static class OpenFile
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070620L;

        private final Component parent;

        private final StrataCozy strataCozy;

        private final JTree tree;

        private final Operation insert;

        private final Operation remove;

        public OpenFile(Component parent, StrataCozy strataCozy, JTree tree, Operation insert, Operation remove)
        {
            this.parent = parent;
            this.strataCozy = strataCozy;
            this.tree = tree;
            this.insert = insert;
            this.remove = remove;
            putValue(Action.NAME, "Open");
        }

        public void actionPerformed(ActionEvent event)
        {
            JFileChooser chooser = new JFileChooser();
            int choice = chooser.showOpenDialog(null);
            if (choice == JFileChooser.APPROVE_OPTION)
            {
                strataCozy.reset();
                File file = chooser.getSelectedFile();
                try
                {
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    int lineNumber = 1;
                    String line = null;
                    while ((line = reader.readLine()) != null)
                    {
                        char action = line.charAt(0);
                        String value = line.substring(1);
                        int number;
                        try
                        {
                            number = Integer.parseInt(value);
                        }
                        catch (NumberFormatException e)
                        {
                            throw new IOException("Cannot parse number <" + value + "> at line " + lineNumber + ".");
                        }
                        switch (action)
                        {
                        case 'A':
                            insert.operate(number);
                            break;
                        case 'D':
                            remove.operate(number);
                            break;
                        default:
                            throw new IOException("Unknown action <" + action + "> at line " + lineNumber + ".");
                        }
                        strataCozy.getStrata().query(null).copacetic();
                        lineNumber++;
                    }
                }
                catch (IOException e)
                {
                    JOptionPane.showMessageDialog(parent, "Cannot Open File", e.getMessage(), JOptionPane.ERROR_MESSAGE);
                }
            }

            TreeModelStorage model = (TreeModelStorage) tree.getModel();
            model.fire();

            int i = 0;
            while (i < tree.getRowCount())
            {
                tree.expandRow(i);
                i++;
            }
        }
    }

    private final static class CopyTest
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070620L;

        private final StringBuffer tests;

        public CopyTest(StringBuffer tests)
        {
            this.tests = tests;
            putValue(Action.NAME, "Copy Test To Clipboard");
        }

        public void actionPerformed(ActionEvent arg0)
        {
            Clipboard system = Toolkit.getDefaultToolkit().getSystemClipboard();
            StringSelection selection = new StringSelection(tests.toString());
            system.setContents(selection, selection);
        }
    }

    private final static class AddNumber
    extends AbstractAction
    {
        private static final long serialVersionUID = 20070620L;

        private final JTextField entry;

        private final JTree tree;

        private final Operation add;

        public AddNumber(JTextField entry, JTree tree, Operation add)
        {
            this.entry = entry;
            this.tree = tree;
            this.add = add;
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

            add.operate(number);

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

        private final Operation remove;

        public RemoveNumber(JTree tree, Operation remove)
        {
            this.tree = tree;
            this.remove = remove;
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

                    remove.operate(number);

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

    public final static void main(String[] args)
    {
        JFrame frame = new JFrame("Hello, World");

        frame.setLayout(new BorderLayout());

        StrataCozy strataCozy = new StrataCozy(2);

        JPanel panel = new JPanel();

        JTextField entry = new JTextField(10);
        JButton add = new JButton("Add");

        panel.add(entry);
        panel.add(add);

        StringBuffer actions = new StringBuffer();
        StringBuffer tests = new StringBuffer();

        frame.add(panel, BorderLayout.NORTH);

        JTree tree = new JTree(strataCozy.getTreeModel());
        Operation remove = new Remove(strataCozy, actions, tests);
        tree.addMouseListener(new RemoveNumber(tree, remove));
        tree.setRootVisible(false);
        frame.add(new JScrollPane(tree), BorderLayout.CENTER);

        Operation insert = new Insert(strataCozy, actions, tests);
        AddNumber addNumber = new AddNumber(entry, tree, insert);
        entry.getActionMap().put("addNumber", addNumber);
        entry.getInputMap().put(KeyStroke.getKeyStroke("ENTER"), "addNumber");
        add.addActionListener(addNumber);

        JMenuBar menuBar = new JMenuBar();
        JMenu menu = new JMenu("File");
        menuBar.add(menu);

        JMenuItem save = new JMenuItem(new SaveFile(frame, actions));
        menu.add(save);
        JMenuItem open = new JMenuItem(new OpenFile(frame, strataCozy, tree, insert, remove));
        menu.add(open);
        
        menu.addSeparator();
        
        JMenuItem size = new JMenuItem(new SetSize(strataCozy, tree));
        menu.add(size);

        menu.addSeparator();

        JMenuItem copyTests = new JMenuItem(new CopyTest(tests));
        menu.add(copyTests);

        frame.setJMenuBar(menuBar);

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