/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

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
import javax.swing.SpinnerNumberModel;
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
        public final CharacterTier tier;

        public final Character text;
        
        public final boolean leaf;

        public TreeNode(CharacterTier tier, Character text, boolean leaf) {
            this.tier = tier;
            this.text = text;
            this.leaf = leaf;
        }

        public String toString() {
            return text.toString();
        }
    }

    private final static class StrataTreeModel implements TreeModel {
        private final static long serialVersionUID = 20070613L;

        private CharacterTier root;

        private final EventListenerList listeners = new EventListenerList();

        public StrataTreeModel(CharacterTier root) {
            this.root = root;
        }

        public void addTreeModelListener(TreeModelListener listener) {
            listeners.add(TreeModelListener.class, listener);
        }

        public void removeTreeModelListener(TreeModelListener listener) {
            listeners.remove(TreeModelListener.class, listener);
        }

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

    private interface Operation {
        public void operate(Character ch);
    }

    private final static class Insert implements Operation {
        private final StrataCozy strataCozy;

        private final StringBuffer actions;

        public Insert(StrataCozy strataCozy, StringBuffer actions) {
            this.strataCozy = strataCozy;
            this.actions = actions;
        }

        public void operate(Character ch) {
            actions.append(ch);
            strataCozy.getStrata().query(null).add(ch);
            strataCozy.getStrata().query(null).copacetic();
        }
    }

    private final static class Remove
    implements Operation {
        private final StrataCozy strataCozy;

        private final StringBuffer actions;

        private final StringBuffer tests;

        public Remove(StrataCozy strataCozy, StringBuffer actions, StringBuffer tests) {
            this.strataCozy = strataCozy;
            this.actions = actions;
            this.tests = tests;
        }

        public void operate(Character ch) {
            actions.append("D" + ch + "\n");
            tests.append("query.remove(new Integer(" + ch + "));\n");

            strataCozy.getStrata().query(null).remove(ch);
            strataCozy.getStrata().query(null).copacetic();
        }
    }

    private final static class SaveFile
    extends AbstractAction {
        private static final long serialVersionUID = 20070620L;

        private final Component parent;

        private final StringBuffer buffer;

        public SaveFile(Component parent, StringBuffer buffer) {
            this.parent = parent;
            this.buffer = buffer;
            putValue(Action.NAME, "Save");
        }

        public void actionPerformed(ActionEvent event) {
            JFileChooser chooser = new JFileChooser();
            int choice = chooser.showSaveDialog(null);
            if (choice == JFileChooser.APPROVE_OPTION) {
                File file = chooser.getSelectedFile();
                FileWriter writer;
                try {
                    writer = new FileWriter(file);
                    writer.write(buffer.toString());
                    writer.close();
                } catch (IOException e) {
                    JOptionPane.showMessageDialog(parent, "Cannot write file.");
                }
            }
        }
    }

    private final static class SetSize
    extends AbstractAction {
        private static final long serialVersionUID = 20070729L;

        private final StrataCozy strataCozy;

        private final JTree tree;

        public SetSize(StrataCozy strataCozy, JTree tree) {
            this.strataCozy = strataCozy;
            this.tree = tree;
            putValue(Action.NAME, "Set Tier Size...");
        }

        public void actionPerformed(ActionEvent arg0) {
            SpinnerNumberModel number = new SpinnerNumberModel();
            number.setMinimum(new Integer(2));
            number.setValue(new Integer(strataCozy.getSize()));
            String[] options = new String[] { "OK", "Cancel" };
            int result = JOptionPane.showOptionDialog(null, new Object[] { "Set size of Strata tiers.", new JSpinner(number) }, "Set Tier Size", JOptionPane.OK_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, options, options[0]);
            if (result == 0) {
                strataCozy.setSize(((Integer) number.getValue()).intValue());
            }

            StrataTreeModel model = (StrataTreeModel) tree.getModel();
            model.fire();

            int i = 0;
            while (i < tree.getRowCount()) {
                tree.expandRow(i);
                i++;
            }
        }
    }

    private final static class StrataCozy {
        private Strata<Character> strata;

        private int size;

        public StrataCozy(JTree tree, int size) {
            setSize(size);
        }

        public void setSize(int size) {
            Schema<Character> schema = new Schema<Character>();
            schema.setInnerCapacity(size);
            schema.setLeafCapacity(size);
            schema.setComparableFactory(new CastComparableFactory<Character>());
            CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
            this.strata = schema.open(new Stash(), address, new CharacterTierStorage());

            this.size = size;
        }

        public void reset() {
            setSize(getSize());
        }

        public int getSize() {
            return size;
        }

        public Strata<Character> getStrata() {
            return strata;
        }
    }

    private final static class OpenFile
    extends AbstractAction {
        private static final long serialVersionUID = 20070620L;

        private final Component parent;

        private final StrataCozy strataCozy;

        private final JTree tree;

        private final Operation insert;

        private final Operation remove;

        public OpenFile(Component parent, StrataCozy strataCozy, JTree tree, Operation insert, Operation remove) {
            this.parent = parent;
            this.strataCozy = strataCozy;
            this.tree = tree;
            this.insert = insert;
            this.remove = remove;
            putValue(Action.NAME, "Open");
        }

        public void actionPerformed(ActionEvent event) {
            JFileChooser chooser = new JFileChooser();
            int choice = chooser.showOpenDialog(null);
            if (choice == JFileChooser.APPROVE_OPTION) {
                strataCozy.reset();
                File file = chooser.getSelectedFile();
                try {
                    BufferedReader reader = new BufferedReader(new FileReader(
                            file));
                    int lineNumber = 1;
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        char action = line.charAt(0);
                        String value = line.substring(1);
                        char number = value.charAt(0);
                        switch (action) {
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
                } catch (IOException e) {
                    JOptionPane.showMessageDialog(parent, "Cannot Open File", e.getMessage(), JOptionPane.ERROR_MESSAGE);
                }
            }

            StrataTreeModel model = (StrataTreeModel) tree.getModel();
            model.fire();

            int i = 0;
            while (i < tree.getRowCount()) {
                tree.expandRow(i);
                i++;
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

    private final static class RemoveNumber
    extends MouseAdapter {
        private final JTree tree;

        private final Operation remove;

        public RemoveNumber(JTree tree, Operation remove) {
            this.tree = tree;
            this.remove = remove;
        }

        public void mousePressed(MouseEvent event) {
            int selRow = tree.getRowForLocation(event.getX(), event.getY());
            TreePath selPath = tree.getPathForLocation(event.getX(), event.getY());
            if (selRow != -1 && event.getClickCount() == 2) {
                StrataTreeModel model = (StrataTreeModel) tree.getModel();
                Object object = selPath.getLastPathComponent();
                if (model.isLeaf(object) && !"<".equals(object)) {
                    String value = object.toString();
                    int space = value.indexOf(' ');
                    if (space != -1) {
                        value = value.substring(0, space);
                    }

                    char number = value.charAt(0);

                    remove.operate(number);

                    model.fire();

                    int i = 0;
                    while (i < tree.getRowCount()) {
                        tree.expandRow(i);
                        i++;
                    }
                }
            }
        }
    }

    public final static void main(String[] args) {
        JFrame frame = new JFrame("Hello, World");

        frame.setLayout(new BorderLayout());


        JPanel panel = new JPanel();

        StringBuffer actions = new StringBuffer();
        StringBuffer tests = new StringBuffer();

        frame.add(panel, BorderLayout.NORTH);

        Schema<Character> schema = new Schema<Character>();
        schema.setInnerCapacity(4);
        schema.setLeafCapacity(4);
        schema.setComparableFactory(new CastComparableFactory<Character>());
        CharacterTier address = schema.create(new Stash(), new CharacterTierStorage());
        final Strata<Character> strata =  schema.open(new Stash(), address, new CharacterTierStorage());

        final StrataTreeModel model = new StrataTreeModel(address);
        final JTree tree = new JTree(model);
        StrataCozy strataCozy = new StrataCozy(tree, 2);
        Operation remove = new Remove(strataCozy, actions, tests);
        tree.addMouseListener(new RemoveNumber(tree, remove));
        tree.setRootVisible(false);
        frame.add(new JScrollPane(tree), BorderLayout.CENTER);

        Operation insert = new Insert(strataCozy, actions);
//        AddNumber addNumber = new AddNumber(entry, tree, insert);
//        entry.getActionMap().put("addNumber", addNumber);
//        entry.getInputMap().put(KeyStroke.getKeyStroke("ENTER"), "addNumber");
//        add.addActionListener(addNumber);
        final JTextField editor = new JTextField(20);
        editor.getDocument().addDocumentListener(new DocumentListener() {
            public void removeUpdate(DocumentEvent e) {
                System.out.println("Called");
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

        JMenuItem save = new JMenuItem(new SaveFile(frame, actions));
        menu.add(save);
        JMenuItem open = new JMenuItem(new OpenFile(frame, strataCozy, tree, insert, remove));
        menu.add(open);

        menu.addSeparator();

        JMenuItem size = new JMenuItem(new SetSize(strataCozy, tree));
        menu.add(size);

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