// Java implementation for a client
// Save file as Client.java

import java.io.*;
import java.net.*;

import java.util.*;


// Client class
public class TCPClient
{
	public static List<String> activeServerList = new ArrayList<String>(Arrays.asList("dc01.utdallas.edu","dc02.utdallas.edu","dc03.utdallas.edu"));
	public static Map<String,String> activeClientIdList = new HashMap<String,String>(){{
		put("dc04.utdallas.edu","Client 1");
		put("dc05.utdallas.edu","Client 2");
		put("dc06.utdallas.edu","Client 3");
	}};
	public static void main(String[] args) throws IOException
	{
		try
		{
			
			System.out.println(activeServerList);
			String serverAddress = "dc01.utdallas.edu";//activeServerList.get(new Random().nextInt(activeServerList.size()));
			System.out.println("Server Address = " + serverAddress);
			Scanner scn = new Scanner(System.in);
			
			// getting localhost ip
			 InetAddress sAddress = InetAddress.getByName("localhost");
			//InetAddress sAddress = InetAddress.getLocalHost();
			
			String clientId = "localhost";//activeClientIdList.get(sAddress.getHostName());
			System.out.println("Client ID = " + clientId);
			// establish the connection with server port 5056
			System.out.println("Enter Port number");
			int port = scn.nextInt();
			Socket s = new Socket("127.0.0.1", port);
	
			// obtaining input and out streams
			DataInputStream dis = new DataInputStream(s.getInputStream());
			DataOutputStream dos = new DataOutputStream(s.getOutputStream());
			boolean isTerminateRequested = false;
			boolean isFileWriteSuccess = false;
	
			// the following loop performs the exchange of
			// information between client and client handler
			while (true)
			{
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

