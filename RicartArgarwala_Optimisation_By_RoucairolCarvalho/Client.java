// Java implementation for a client
// Save file as Client.java

import java.io.*;
import java.net.*;

import java.util.*;


// Client class
public class Client
{
	public static List<String> ACTIVE_SERVER_LIST = new ArrayList<String>(Arrays.asList("dc01.utdallas.edu","dc02.utdallas.edu","dc03.utdallas.edu"));
	public static Map<String,String> CLIENT_ID_LIST = new HashMap<String,String>(){{
		put("dc30.utdallas.edu","Client 1");
		put("dc31.utdallas.edu","Client 2");
		put("dc32.utdallas.edu","Client 3");
		put("dc33.utdallas.edu","Client 4");
		put("dc34.utdallas.edu","Client 5");
	}};
	public static String METADATA = null;
	public static void main(String[] args) throws IOException
	{
		try
		{
			
			String serverAddress = ACTIVE_SERVER_LIST.get(new Random().nextInt(ACTIVE_SERVER_LIST.size()));
			System.out.println("Connected to Server = " + serverAddress);
			Scanner scn = new Scanner(System.in);
			
			// getting localhost ip
			//InetAddress ip = InetAddress.getByName("localhost");
			InetAddress sAddress = InetAddress.getLocalHost();
			
			String clientId = CLIENT_ID_LIST.get(sAddress.getHostName());
			System.out.println("Client ID = " + clientId);
			// establish the connection with server port 4056
			int port = 4056;
			Socket s = new Socket(serverAddress, port);
	
			// obtaining input and out streams
			DataInputStream dis = new DataInputStream(s.getInputStream());
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			boolean isTerminateRequested = false;
			boolean isFileWriteSuccess = false;
	
			// the following loop performs the exchange of
			// information between client and client handler
			while (true)
			{	
				System.out.println("METADATA : " + METADATA);
				dos.writeUTF(clientId);
				System.out.println(dis.readUTF());
				String userInput = scn.nextLine();
				//dos.writeUTF(tosend);
				//dos.writeUTF(java.time.Clock.systemUTC().instant().toString());
				// If client sends exit,close this connection
				// and then break from the while loop
				
				switch(userInput){
					case "1" :
						dos.writeUTF(userInput);
						String fileListString = dis.readUTF();
						METADATA = fileListString;
						String[] fileArray  = fileListString.split(",");
						for(String a : fileArray ){
							System.out.println(a.trim());
						}
						break;
					case "2" : 
						dos.writeUTF(userInput);
						System.out.println(dis.readUTF());
						String fileName = scn.nextLine();
						String message = fileName + "," + java.time.Clock.systemUTC().instant().toString()+ "," + clientId;
						dos.writeUTF(message);
						isFileWriteSuccess = dis.readBoolean();
						if(!isFileWriteSuccess){
							isTerminateRequested = true;
						}
						break;
					case "E" :
						dos.writeUTF(userInput);
						isTerminateRequested = true;
						break;
					default :
						System.out.println("Invalid Input. Enter Again");
						break;
				}
				
				// printing date or time as requested by client
				if(isTerminateRequested || isFileWriteSuccess){
					String serverMessage = dis.readUTF();
					String message  = serverMessage!=null ? serverMessage:"Client End Terminated";
					System.out.println(message);
					s.close();
					break;
				}
				
			}
			
			// closing resources
			scn.close();
			dis.close();
			dos.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}

