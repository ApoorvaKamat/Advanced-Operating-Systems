// Java implementation of Server side
// It contains two classes : Server and ClientHandler
// Save file as TCPServer.java

import java.io.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;

// Server class
public class TCPServer extends Thread
{
	/**
	 * GLOBAL CONSTANTS
	 */
	public static Integer PORT_NUMBER = 4056;
	public static String SERVER_NAME;
    public static CountDownLatch latch   =  new CountDownLatch(1);

 	// Global  Declarations
	
	public static Map<String,String> SERVER_FOLDER_MAP = new HashMap<String, String>(){{
		put("dc01.utdallas.edu", "Server0");
		put("dc02.utdallas.edu", "Server1");
		put("dc03.utdallas.edu", "Server2");
	}};

	@Override
	public void run(){

	}

	public static void main(String[] args) throws IOException
	{
		//Start two threads 
		//Thread 1 : listens to client port 5066
		//Thread 2 : listens to server port 2212

		try
		{
			InetAddress sAddress = InetAddress.getLocalHost();
			SERVER_NAME = sAddress.getHostName();
			//Server Listening to client requests on port 4056
			//Started the server
			while(true){
				System.out.println("Server Listening to client requests on port 4056");
				ServerSocket clientSocket = new ServerSocket(PORT_NUMBER);
				try {
					Socket sClient = clientSocket.accept();
					//System.out.println("Connect accepted: Starting new thread");
					Thread handler = new RequestResposneHandler(sClient,latch);
					handler.start();
					latch.await();
					System.out.println("Thread ends");
				} catch(IOException i){
					clientSocket.close();
					i.printStackTrace();
				}
				catch (Exception e) {
					clientSocket.close();
					e.printStackTrace();
				}
				finally{
					
					clientSocket.close();
					System.out.println("Socket Closed");
				}
				
			}

		}catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("8");

	}
}

class RequestResposneHandler extends Thread {
	private final Socket _sClient;
	private final CountDownLatch  _latch;
	
	public RequestResposneHandler(Socket sClient, CountDownLatch latch){
		_sClient = sClient;
		_latch = latch;
		
	}

	synchronized private static boolean PerformSynchronousWrite(String message){
		try {
			String serverName = TCPServer.SERVER_FOLDER_MAP.get(TCPServer.SERVER_NAME);
			Path filePath = Paths.get(System.getProperty("user.dir"),serverName.trim(),"F1.txt");
			File file = new File(filePath.toString());
			FileOutputStream fOutputStream =  new FileOutputStream(file,true);
			byte[] messageByte = message.getBytes();
			fOutputStream.write(messageByte);
			fOutputStream.close();
		} catch(IOException i)
		{
			System.out.println(i);
			return false;
		}
		catch(Exception e){

			System.out.println(e);
			return false;
		}
		return true;
	}

	@Override
	public void run(){
		//boolean run = true;
		try {
			//System.out.println("3");
			BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
			while(true){
				try
				{
					//System.out.println("4");
					//Receive incomming requests
					System.out.println("Connect Accpeted");
					
					DataInputStream dis = new DataInputStream(_sClient.getInputStream());
					DataOutputStream dos = new DataOutputStream(_sClient.getOutputStream());
					String userInput = null;
					int clientId = dis.readInt();
					//System.out.println("Request received from Client : " + clientId );
					String req1 = dis.readUTF();
					if(req1.equalsIgnoreCase("PREPARE")){
						System.out.println("Prepare Received from Client : "  + clientId);
						System.out.println("R - Ready\n A - Abort \n");
						userInput = bf.readLine();
						dos.writeBoolean(userInput.equalsIgnoreCase("r"));
						if(userInput.equalsIgnoreCase("r")){
							System.out.println("Sent Ready : "  + clientId);
						}
						else{
							System.out.println("Sent Abort : "  + clientId);
						}
					} 
					boolean canCommit = dis.readBoolean();  
					if(canCommit){
						System.out.println("Commit Received from Client : "  + clientId);
						String message  = dis.readUTF();
						Boolean isWriteSuccess = PerformSynchronousWrite(message);
						dos.writeBoolean(isWriteSuccess);
					}
					else{
						System.out.println("Abort Received from Client : "  + clientId);
					}
					//System.out.println("5");
					dos.close();
					dis.close();
					
					_sClient.close();
					//run = false;
					break;
					
	
				}
				catch(IOException i){
					_sClient.close();
					i.printStackTrace();
				}
				catch (Exception e) {
					_sClient.close();
					e.printStackTrace();
				}
			}
			System.out.println("6");
		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//System.out.println("7");
		_latch.countDown();
	}
}
