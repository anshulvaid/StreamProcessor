import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

public class Client {
	private BufferedReader in;
    private PrintWriter out;
    private JFrame frame = new JFrame("TQL Stream Client");
    private JTextField dataField = new JTextField("Enter query here....",40);
    private JTextArea messageArea = new JTextArea(40, 100);

    public Client() {
        messageArea.setEditable(false);
        frame.getContentPane().add(dataField, "North");
        frame.getContentPane().add(new JScrollPane(messageArea), "Center");

        dataField.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                out.println(dataField.getText());
            }
        });
    }

    public void getConnection() throws IOException {
        Socket socket = new Socket("127.0.0.1", 8200);
        in = new BufferedReader(
                new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
    }
    
    public void queryServer(){
    	String response;
    	dataField.selectAll();
    	while(true){
    		try {
                response = in.readLine();
                if (response == null){
                	break;
                }else if(response.equals("")){
                	messageArea.append("Blank response received.");
                }
            } catch (IOException ex) {
                   response = "Error: " + ex;
            }
            messageArea.append(response + "\n");
            dataField.selectAll();
    	}
    }
    
    public static void main(String[] args) throws Exception {
        Client cl = new Client();
        cl.frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        cl.frame.pack();
        cl.frame.setVisible(true);
        cl.getConnection();
        cl.queryServer();
    }


}
