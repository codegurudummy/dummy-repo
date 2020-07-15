package resourceleak;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class TestResourcesClosedIfNotNull {

    DataInputStream fromServer = null;
    DataOutputStream toServer = null;

    @Override // Override the start method in the Application class
    public void testResourcesClosedIfNotNull(Stage primaryStage) {
        Socket socket;
        try {
            // Create a socket to connect to the server
            socket = new Socket("localhost", 8000);

            // Create an input stream to receive data from the server
            fromServer = new DataInputStream(socket.getInputStream());

            // Create an output stream to send data to the server
            toServer = new DataOutputStream(socket.getOutputStream());
        }
        catch (IOException ex) {
            ta.appendText(ex.toString() + '\n');
        }
        finally {
            if (socket != null) {
                socket.close();
            }
        }
    }
}
