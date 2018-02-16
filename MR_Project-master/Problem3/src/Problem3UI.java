import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.border.EmptyBorder;
import javax.swing.border.CompoundBorder;
import javax.swing.border.BevelBorder;
import javax.swing.border.TitledBorder;
import javax.swing.border.EtchedBorder;
import java.awt.Color;
import javax.swing.JLabel;

public class Problem3UI extends JFrame {

	private JLabel labelDayOfWeek = new JLabel("Day of Week: ");
    private JLabel labelOrigin = new JLabel("Origin: ");
    private JLabel labelDestination = new JLabel("Destination: ");
    private JLabel labelNumberOfStops = new JLabel("Number of Stops: ");    
    private JButton buttonSubmit = new JButton("Submit");
    private JLabel statusLabel;
    public Problem3UI() {
        super("JPanel Demo Program");
         
        // create a new panel with GridBagLayout manager
        JPanel newPanel = new JPanel(new GridBagLayout());
         
        GridBagConstraints constraints = new GridBagConstraints();
        constraints.anchor = GridBagConstraints.WEST;
        constraints.insets = new Insets(10, 10, 10, 10);
         
        // add components to the panel
        constraints.gridx = 0;
        constraints.gridy = 0;     
        newPanel.add(labelDayOfWeek, constraints);
 
        constraints.gridx = 1;
        final DefaultComboBoxModel weekDays = new DefaultComboBoxModel();
        
        weekDays.addElement("Sunday");
        weekDays.addElement("Monday");
        weekDays.addElement("Tuesday");
        weekDays.addElement("Wednesday");
        weekDays.addElement("Thursday");
        weekDays.addElement("Friday");
        weekDays.addElement("Saturday");
        
        

        final JComboBox weekCombo = new JComboBox(weekDays);    
        weekCombo.setSelectedIndex(0);

        JScrollPane daysListScrollPane = new JScrollPane(weekCombo);    
        newPanel.add(daysListScrollPane, constraints);
         
        constraints.gridx = 0;
        constraints.gridy = 1;     
        newPanel.add(labelOrigin, constraints);
         
        constraints.gridx = 1;
        final DefaultComboBoxModel airports = new DefaultComboBoxModel();
        airports.addElement("Airport1");
        
        final JComboBox airportCombo = new JComboBox(airports);    
        airportCombo.setSelectedIndex(0);

        JScrollPane airportListScrollPane = new JScrollPane(airportCombo);    
        newPanel.add(airportListScrollPane, constraints);
        
        constraints.gridx = 0;
        constraints.gridy = 2;     
        newPanel.add(labelDestination, constraints);
         
        constraints.gridx = 1; 
        final JComboBox airportCombo1 = new JComboBox(airports);    
        airportCombo1.setSelectedIndex(0);
        JScrollPane airportListScrollPane1 = new JScrollPane(airportCombo1); 
        newPanel.add(airportListScrollPane1, constraints);
        
        constraints.gridx = 0;
        constraints.gridy = 3;     
        newPanel.add(labelNumberOfStops, constraints);
        
        constraints.gridx = 1; 
        final JCheckBox chkApple = new JCheckBox("0 Stop");
        final JCheckBox chkMango = new JCheckBox("1 Stop");
        newPanel.add(chkApple, constraints);
        constraints.gridx = 2; 
        newPanel.add(chkMango, constraints);
        
        constraints.gridx = 0;
        constraints.gridy = 4;
        constraints.gridwidth = 2;
        constraints.anchor = GridBagConstraints.CENTER;
        newPanel.add(buttonSubmit, constraints);
        
        constraints.gridx = 0;
        constraints.gridy = 5;
        statusLabel = new JLabel("",JLabel.CENTER);    

        statusLabel.setSize(350,100);
        newPanel.add(statusLabel, constraints);
        
        buttonSubmit.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) { 
               String day = "";
               String origin = "";
               String destination = "";
               if (weekCombo.getSelectedIndex() != -1) {                     
                  day = (weekCombo.getItemAt(weekCombo.getSelectedIndex())).toString();             
               }
               if (airportCombo.getSelectedIndex() != -1) {                     
                   origin = (airportCombo.getItemAt(airportCombo.getSelectedIndex())).toString();             
                }
               if (airportCombo1.getSelectedIndex() != -1) {                     
                   destination = (airportCombo1.getItemAt(airportCombo1.getSelectedIndex())).toString();             
                }  
            }
         }); 
         
        // set border for the panel
        newPanel.setBorder(BorderFactory.createTitledBorder(
                BorderFactory.createEtchedBorder(), "Best Flight Finder"));
         
        // add the panel to this frame
        add(newPanel);
         
        pack();
        //setLocationRelativeTo(null);
    }
     
    public static void main(String[] args) {
        // set look and feel to the system look and feel
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
         
        SwingUtilities.invokeLater(new Runnable() {
            @Override
            public void run() {
                new Problem3UI().setVisible(true);
            }
        });
    }

}
