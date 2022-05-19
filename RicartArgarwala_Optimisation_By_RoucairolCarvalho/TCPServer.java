// Java implementation of Server side
// It contains two classes : Server and ClientHandler
// Save file as TCPServer.java

import java.io.*;
import java.text.*;
import java.util.*;
import java.net.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;

// Server class
public class TCPServer
{
	/**
	 * GLOBAL CONSTANTS
	 */
	public static  int SERVER_PORT_NUMBER = 2212;
	public static  int CLIENT_PORT_NUMBER = 4056;
	public static String SERVER_NAME;


 	// Global  Declarations
	public static List<Timestamp> DEFERED_QUEUE = new ArrayList<Timestamp>();
	public static List<String> SELF_MESSAGE_QUEUE = new ArrayList<String>();
	public static volatile Timestamp CURRENT_REQUEST_TS;
	public static List<String> ACTIVE_SERVERS_LIST = new ArrayList<String>(Arrays.asList(
	"dc01.utdallas.edu",
	"dc02.utdallas.edu",
	"dc03.utdallas.edu"));
	public static volatile Map<String, Boolean> FILE_LOCK_INFO = new HashMap<String, Boolean>(){{
		put("F1.txt", false);
		put("F2.txt", false);
	}};
	
	public static volatile Map<String, Boolean> GLOBAL_FILE_LOCK_PERMISSION = new HashMap<String, Boolean>(){{
		put("F1.txt", false);
		put("F2.txt", false);
	}};
	
	public static Map<String,String> SERVER_FOLDER_MAP = new HashMap<String, String>(){{
		put("dc01.utdallas.edu", "Server0");
		put("dc02.utdallas.edu", "Server1");
		put("dc03.utdallas.edu", "Server2");
	}};



	public static void main(String[] args) throws IOException
	{
		//Start two threads 
		//Thread 1 : listens to client port 5066
		//Thread 2 : listens to server port 2212

		try
		{
			InetAddress sAddress = InetAddress.getLocalHost();
			SERVER_NAME = sAddress.getHostName();
			Thread clientThread = new ClientListener();
			Thread serverThread = new ServerListener();

			//Invoking Start 
			serverThread.start();
			clientThread.start();
		}
		catch(Exception e){
			e.printStackTrace();
		}


	}
}

class ClientListener extends Thread
{

 	public ClientListener(){
	}
	@Override
	public void run(){
		//Server Listening to client requests on port 4056
		System.out.println("Server Listening to client requests on port 4056");
		try (ServerSocket clientSocket = new ServerSocket(TCPServer.CLIENT_PORT_NUMBER)) {

			//Running infinte loop for getting Client Requests
			while(true){
				Socket sClient = null;
				try
				{
					//Receive incomming requests
					sClient = clientSocket.accept();

					DataInputStream dis = new DataInputStream(sClient.getInputStream());
					DataOutputStream dos = new DataOutputStream(sClient.getOutputStream());

					System.out.println("Assigning new thread for this client");

					Thread clientHandler = new ClientRequestHandler(sClient, dis, dos);
					clientHandler.start();

				}
				catch(Exception e ){
					sClient.close();
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

class ServerListener extends Thread
{

	public ServerListener(){
		//Empty Constructor
	}
	@Override
	public void run(){
		System.out.println("Server Listening to server requests on port 2212");
		try (ServerSocket serverSocket = new ServerSocket(TCPServer.SERVER_PORT_NUMBER)) {
			while(true)
			{
				Socket sServer = null;

				try
				{
					sServer = serverSocket.accept();
					DataInputStream dis = new DataInputStream(sServer.getInputStream());
					DataOutputStream dos = new DataOutputStream(sServer.getOutputStream());

					System.out.println("Server Connect Accepted\n Assigning New thread");

					Thread serverHandler  = new ServerRequestHandler(sServer, dis, dos);
					serverHandler.start();
					
					

				}
				catch(EOFException e){
					sServer.close();
					e.printStackTrace();
				}
				catch(Exception e)
				{
					sServer.close();
					e.printStackTrace();
				}

			}		
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

/**
 * Class ServerRequestHandler
 * Used to handle SYNCHRONOUS WRITE events
 * Function Discription:
 * 	HandlerServerRequests() :-
 *  1. Reads The message
 *  2. Checks if the file is locked
 * 	3. If Locked - Adds message to the DeferQueue and Waits for file lock to be released
 * 	4. Once lock is released/ no was present  initaially - 
 * 		a. Sends Reply to the server that requested File WRITE
 * 		b. writes the message to its file system
 * 		c. clears the DeferQueue
 * PerformSynchronousWrite:
 * 	1. writes the message into its file system
 */

class ServerRequestHandler extends Thread
{
	final DataInputStream _dis;
	final DataOutputStream _dos;
	final Socket _sServer;
	final String  _relativePath ;

	public ServerRequestHandler(Socket sServer, DataInputStream dis, DataOutputStream dos){
		_dis = dis;
		_dos = dos;
		_sServer = sServer;
		_relativePath = System.getProperty("user.dir");

	}
	synchronized private boolean PerformSynchronousWrite(String fileName, String data ,String clientID) throws IOException{
		try{
			TCPServer.FILE_LOCK_INFO.put(fileName, true);
			System.out.println(fileName + " Locked");
			Path filePath = Paths.get(_relativePath,TCPServer.SERVER_FOLDER_MAP.get(TCPServer.SERVER_NAME),fileName);
			File file = new File(filePath.toString());
			FileOutputStream fOutputStream =  new FileOutputStream(file,true);
			
			
			String message = "<" + data + "," + clientID + ">\n" ;
			byte[] messageByte = message.getBytes();
			fOutputStream.write(messageByte);
			fOutputStream.close();
			Thread.currentThread();
			Thread.sleep(10000);
			TCPServer.FILE_LOCK_INFO.put(fileName, false);
			System.out.println(fileName+" Lock Released");
			System.out.println("Closing file connection");
			return true;
			
		}
		catch(IOException i)
		{
			System.out.println(i);
			return false;
		}
		catch(InterruptedException e){

			System.out.println(e);
			return false;
		}

	}
	synchronized private  void HandlerServerRequests()throws EOFException,IOException{
		try {
				//Commenting to keep One message delay between Servers
				// String message = _dis.readUTF();
				// System.out.println("Message Received " + message);
				// _dos.writeUTF("I am Listening to you!");

				//get File Name for Critical Section
				String[] data = _dis.readUTF().split(",");

				String fileName = data[0].trim();
				Timestamp reqTS = Timestamp.valueOf(data[1].trim());
				String message = data[2].trim();
				String clientId = data[3].trim();

				//Check if Server has global token for the file

				if(TCPServer.GLOBAL_FILE_LOCK_PERMISSION.get(fileName)){
					System.out.println("I have the Global Token");
					// System.out.println("request Time stamp : " + reqTS);
					// System.out.println("Self time Stamp: " +  TCPServer.CURRENT_REQUEST_TS);
					if(reqTS.before(TCPServer.CURRENT_REQUEST_TS)){
						System.out.println("Releasing Global Token for file :" + fileName);
						TCPServer.FILE_LOCK_INFO.put(fileName, false);
						TCPServer.GLOBAL_FILE_LOCK_PERMISSION.put(fileName, false);
					}
					else{
						System.out.println("Deffering request" );
						TCPServer.DEFERED_QUEUE.add(reqTS);
					}

				}
				while(TCPServer.FILE_LOCK_INFO.get(fileName)){
					//System.out.println("Waiting for lock to be released");
				}
				while(!TCPServer.FILE_LOCK_INFO.get(fileName)){
					System.out.println("I dont have the global token");
					System.out.println("Allowing Entry to Critical Section");
					_dos.writeBoolean(true);
					Boolean isSyncWriteSucess = PerformSynchronousWrite(fileName,message,clientId);
					System.out.println("Sync write to " + fileName + " : " + isSyncWriteSucess);
					TCPServer.DEFERED_QUEUE.clear();
					break;
				}
			
		} catch (EOFException e) {
			_sServer.close();
			e.printStackTrace();
		}
		catch (IOException i){
			_sServer.close();
			i.printStackTrace();
		}
		catch(Exception e){
			_sServer.close();
			e.printStackTrace();
		}

	}
	@Override 
	public void run() {
		while(true){
			try {
				
				HandlerServerRequests();
				break;
			} 
			catch(EOFException e){
				System.out.println("Closing Thread");
				e.printStackTrace();
			}
			catch(IOException i){
				System.out.println("Closing Thread");
				i.printStackTrace();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			_sServer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
/**
 * Class ClientRequestHandler
 * Used to handle CLIENT write and Read operations events
 * operations Handling mechanism:
 * 	1. READ/Enquire
 * 		a. returns the list of files in the server.
 * 	2. WRITE
 * 		a. Gets the message 
 * 		b. Checks if the server has the permission to lock the file
 * 		c. if yes
 * 			i. Checks if there is any request in the defered queue
 * 			ii. if yes checks if its current request timestamp is before the requested time stamp
 * 			iii. if yes then enters Critical section
 * 			iv. if No then adds its message to the queue and releases the Global permission
 * 		d. if NO --> 
 * 			- requests reply from other servers to enter cs.
 * Function Discription:
 * 	getFileList() :-
 *  1. Gets The list of files present in the current server and returns the same to the Client
 *  
 * getServerReplies():
 * 	1. Queries the server --> Iff it does not have the file write permission
 *  2. Gathers the replies from all the other serves and returns true if all servers have replied.
 * 	3. Once all servers reply --->sets the Global file lock on to enter Critical section
 *
 * HandleReceivedMessage:
 * 	1. writes the message into its file system
 * 
 */

class ClientRequestHandler extends Thread 
{
	final DataInputStream _dis;
	final DataOutputStream _dos;
	final Socket _sClient;
	final String _relativePath;
	public ClientRequestHandler(Socket sClient, DataInputStream dis, DataOutputStream dos) {
		_sClient = sClient;
		_dis = dis;
		_dos = dos;
		_relativePath = System.getProperty("user.dir");

		
	}

	@Override
	public void run() 
	{
		while (true) 
		{
			try
			{
				//Request user Information
				System.out.println("Requesting user Information from :" + _dis.readUTF());

				_dos.writeUTF("Type\n"+
							"1 - To get File Details\n"+
							"2 - To write to the file\n" +
							"E - To terminate connection\n");
				
				String request = _dis.readUTF();
				Boolean isFileWriteSuccess = false;
				Boolean isTerminateRequested = false;

				switch(request){
					case "1" :
						_dos.writeUTF(getFileList());
						break;
					case "2" :
						_dos.writeUTF("Enter the File Name");
						String clientData  = _dis.readUTF();
						String[] dataArray = clientData.split(",");
						String fileName = dataArray[0].trim();
						String message = dataArray[1].trim();
						String clientID = dataArray[2].trim();


						//Add request to its queue
						Timestamp ts = new Timestamp(System.currentTimeMillis());
						TCPServer.CURRENT_REQUEST_TS = ts;
						System.out.println("Updating Request Queue : " + ts);


						if(!TCPServer.GLOBAL_FILE_LOCK_PERMISSION.get(fileName)){
							boolean isRepliesReceivedTrue = getServerReplies(ts,fileName,message,clientID);
							TCPServer.GLOBAL_FILE_LOCK_PERMISSION.put(fileName,isRepliesReceivedTrue);
						}


						while(TCPServer.GLOBAL_FILE_LOCK_PERMISSION.get(fileName)){
							//System.out.println("can "+ clientID +" execute Critical Section?" + !(TCPServer.DEFERED_QUEUE != null && !TCPServer.DEFERED_QUEUE.isEmpty()));
							boolean checkIfDeferredQueueisEmpty = !(TCPServer.DEFERED_QUEUE != null && !TCPServer.DEFERED_QUEUE.isEmpty());
							if(!checkIfDeferredQueueisEmpty){
								// System.out.println("Defered Queue TS : " + TCPServer.DEFERED_QUEUE.get(0));
								// System.out.println("Self Request TS : " + ts);
								// System.out.println("Is defered TS smaller " + TCPServer.DEFERED_QUEUE.get(0).before(ts));
								if(TCPServer.DEFERED_QUEUE.get(0).before(ts)){
									System.out.println("Cannot enter Critical Section at the moment for :" + clientID);
									//TO do Convert the list to Hash map
									//Empty the queue once token is received again.
									TCPServer.SELF_MESSAGE_QUEUE.add(message);
									TCPServer.FILE_LOCK_INFO.put(fileName, false);
									TCPServer.GLOBAL_FILE_LOCK_PERMISSION.put(fileName, false);
									while(TCPServer.SELF_MESSAGE_QUEUE != null && !TCPServer.SELF_MESSAGE_QUEUE.isEmpty()){
										System.out.println("Requesting Permission back!");
										boolean isRepliesReceivedTrue = getServerReplies(ts,fileName,message,clientID);
										TCPServer.GLOBAL_FILE_LOCK_PERMISSION.put(fileName,isRepliesReceivedTrue);
										TCPServer.SELF_MESSAGE_QUEUE.clear();
									}

								}
								else{
									System.out.println("Begining CS Exceution for :" + clientID);
									isFileWriteSuccess = HandleReceivedMessage(fileName,message,clientID);
									break;
								}

							}
							else{
								System.out.println("Begining CS Exceution for :" + clientID);
								isFileWriteSuccess = HandleReceivedMessage(fileName,message,clientID);
								break;
							}
						}
						break;
						
					case "E":
						isTerminateRequested = true ;
						break;
				}
				if(isFileWriteSuccess || isTerminateRequested){
					String terminateReason = isFileWriteSuccess? "Write Successfull " : "Terminate Connection" ;
					_dos.writeBoolean(isFileWriteSuccess);
					_dos.writeUTF(terminateReason);
					this._sClient.close();
					System.out.println("Connection Closed");
					break;
				}
			}
			catch(IOException e){
				e.printStackTrace();

			}
		}
	}

	private String getFileList()
	{
		Path filePath = Paths.get(_relativePath,TCPServer.SERVER_FOLDER_MAP.get(TCPServer.SERVER_NAME));
		File file = new File(filePath.toString());
		System.out.println(Arrays.toString(file.list()));
		String fileListString = Arrays.toString(file.list()).replace("[","").replace("]","");
		return (fileListString);
	}
	synchronized private boolean getServerReplies(Timestamp ts, String fileName, String message, String clientID)
	{
		Map<String,Boolean> serverReplies = new HashMap<String, Boolean>();
		for(String nodes : TCPServer.ACTIVE_SERVERS_LIST){
			Boolean isNodeNOTRequestingNode = !nodes.trim().equals(TCPServer.SERVER_NAME.trim());
			//System.out.println("Node Name : "+nodes + "Requesting Server Name : " + TCPServer.SERVER_NAME + " " + isEqual) ;
			if(isNodeNOTRequestingNode){
				System.out.println("Checking for reply from server " + nodes);
				try{
					Socket s = new Socket(nodes,2212);
					DataInputStream dis = new DataInputStream(s.getInputStream());
					DataOutputStream dos = new DataOutputStream(s.getOutputStream());
					while (true)
					{
						// System.out.println("Sending Handshake to server " + nodes);
						// dos.writeUTF("Hi this is " + TCPServer.SERVER_NAME);
						// String reply = dis.readUTF();
						// System.out.println(reply);
						// if(reply != null){
						// 	System.out.println("Is File lock on for " + fileName);
						// 	dos.writeUTF(fileName + "," + ts);
						// 	Boolean canLockFile = dis.readBoolean();
						// 	if(canLockFile){
						// 		serverReplies.put(nodes.trim(), canLockFile);
						// 		s.close();
						// 		System.out.println("Received Reply from " + nodes + ":\n\t\t\t\t" 
						// 							+ reply
						// 							+ "can lock section ? : " 
						// 							+ canLockFile);
						// 		break;
						// 	}
							
						// }

						System.out.println("Can Lock File ? " + fileName);
						dos.writeUTF(fileName + "," + ts + "," + message+ "," + clientID);
						Boolean canLockFile = dis.readBoolean();
						if(canLockFile){
							serverReplies.put(nodes.trim(), canLockFile);
							s.close();
							System.out.println("Received Reply from : " + nodes + "\n" 
												+ "can lock section ? : " + canLockFile);
							break;
						}	
					}
					dis.close();
					dos.close();

				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			
			
		}
		serverReplies.forEach((k,v) -> System.out.println(k + " : " + v));
		int replySize = serverReplies.size();
		int activeServerSize = TCPServer.ACTIVE_SERVERS_LIST.size();
		System.out.println("Reply size : "+ replySize);
		System.out.println("Server size : "+ activeServerSize);
		System.out.println("Is Number of Queries = 2(N-1) : "+ (replySize==activeServerSize-1));
		System.out.println(!serverReplies.containsValue(false));
		if(serverReplies.size() == (TCPServer.ACTIVE_SERVERS_LIST.size()-1) && !serverReplies.containsValue(false)){
			System.out.println ("ALL Replies received");
			return true;
		}
		System.out.println("Need to defer the Message");
		return false;
	}

	synchronized private boolean HandleReceivedMessage(String fileName, String data ,String clientID) throws IOException{
		try{
			TCPServer.FILE_LOCK_INFO.put(fileName, true);
			System.out.println(fileName + " Locked");
			Path filePath = Paths.get(_relativePath,TCPServer.SERVER_FOLDER_MAP.get(TCPServer.SERVER_NAME),fileName);
			File file = new File(filePath.toString());
			FileOutputStream fOutputStream =  new FileOutputStream(file,true);
			
			
			String message = "<" + data + "," + clientID + ">\n" ;
			byte[] messageByte = message.getBytes();
			fOutputStream.write(messageByte);
			fOutputStream.close();
			Thread.currentThread();
			Thread.sleep(10000);
			TCPServer.FILE_LOCK_INFO.put(fileName, false);
			System.out.println(fileName+" Lock Released");
			System.out.println("Closing file connection");
			return true;
			
		}
		catch(IOException i)
		{
			System.out.println(i);
			return false;
		}
		catch(InterruptedException e){

			System.out.println(e);
			return false;
		}

	}

	
}



