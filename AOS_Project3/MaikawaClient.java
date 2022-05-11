// Java implementation for a client
// Save file as Client.java

import java.io.*;
import java.net.*;
import java.nio.charset.MalformedInputException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.sql.Blob;
import java.sql.Time;
import java.sql.Timestamp;

class RQUEST_TYPE {
	public static final String PERMIT = "PERMIT";
	public static final String LOCKED = "LOCKED";
	public static final String FAILED = "FAILED";
	public static final String INQUIRE = "INQUIRE";
	public static final String RELEASE = "RELEASE";
	public static final String YIELD = "YIELD";
}
/**
 * Main Class --> Main Thread
 * Listens and Process requests Made to self
 */
public class MaikawaClient
{
	public static volatile Boolean IS_LOCKED = false;

	public static volatile Timestamp LOCKED_REQ_TS;

	public static volatile Integer LOCKED_CLIENT_ID = null;

	public static volatile Boolean	IS_INQUIRE_SENT = false;

	public static volatile Boolean IS_INQUIRE_RECEIVED = false;

	public static volatile Boolean IS_WRITE_SUCCESS = false;


	public static Integer SERVER_PORT = 4056;

	public static Integer CLIENT_PORT = 2021;

	public static Map<String,Integer> CLIENT_ID_LIST = new HashMap<String,Integer>(){{
		put("dc30.utdallas.edu",0);
		put("dc31.utdallas.edu",1);
		put("dc32.utdallas.edu",2);
		put("dc33.utdallas.edu",3);
		put("dc34.utdallas.edu",4);
	}};
	public static Map<Integer,String> QUORUM_MAP = new HashMap<Integer,String>(){{
		put(0,"dc30.utdallas.edu");
		put(1,"dc31.utdallas.edu");
		put(2,"dc32.utdallas.edu");
		put(3,"dc33.utdallas.edu");
		put(4,"dc34.utdallas.edu");
	}};
	
	public static Map<Integer,ArrayList<Integer>> QUORUMS_LIST = new HashMap<Integer,ArrayList<Integer>>(){{
		put(0, new ArrayList<Integer>(Arrays.asList(0,1,2)));
		put(1, new ArrayList<Integer>(Arrays.asList(1,2,3)));
		put(2, new ArrayList<Integer>(Arrays.asList(2,3,4)));
		put(3, new ArrayList<Integer>(Arrays.asList(3,4,0)));
		put(4, new ArrayList<Integer>(Arrays.asList(4,0,1)));
	}};

	public static volatile HashMap<String,Integer> QUORUM_REPLY = new HashMap<String,Integer>(){{
		put("Locked", 0);
		put("Failed", 0);
	}};

	public static LinkedList<Integer> REQUEST_QUEUE = new LinkedList<Integer>();
	
	public static volatile LinkedHashMap<Timestamp,Integer> WAIT_QUEUE = new LinkedHashMap<Timestamp,Integer>();
	
	//private static final CountDownLatch LATCH = new CountDownLatch(2);
	
	
	/**
	 * addAndSortWaitQueue
	 * Adds Request to the WAIT QUEUE (Outstanding Requests)
	 * Sorts according to TimeStamp -- Lowest Timestamo(i.e Highest Priority First)
	 * @param ts
	 * @param clientId
	 */
	synchronized public static void addAndSortWaitQueue(Timestamp ts, int clientId){
		WAIT_QUEUE.put(ts, clientId);

		LinkedHashMap<Timestamp,Integer> temp_wait_queue = new LinkedHashMap<Timestamp,Integer>();
		Map<Timestamp,Integer> map = new TreeMap<>(WAIT_QUEUE);

		for(Timestamp tkey : map.keySet()){
			temp_wait_queue.put(tkey, WAIT_QUEUE.get(tkey));
		}
		WAIT_QUEUE.clear();
		WAIT_QUEUE = temp_wait_queue;
	}

	public  static void main(String[] args) throws IOException
	{
		BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
		try
		{
			
			InetAddress sAddress = InetAddress.getLocalHost();
			System.out.print(sAddress.getHostName());
			int clientId = CLIENT_ID_LIST.get(sAddress.getHostName());
			System.out.println("Connected to Client " + clientId);
			System.out.println("Quorums Are Client { " + QUORUMS_LIST.get(clientId) + " }");

			Thread.currentThread().setName("Main Thread");
			Thread clientListener = new ClientListener(clientId);
			clientListener.start();

			while(true){
				System.out.println("Running on : " + Thread.currentThread().getName());
				System.out.println("Enter\n"+"2 - To write to the file\n");

				String userInput = bf.readLine();


				switch(userInput){
					case "1" : 
						// dos.writeUTF(userInput);
						// String fileListString  = dis.readUTF();
						// for(String a : fileListString.split(",")){
						// 	System.out.println(a.trim());
						// }
					break;
					case "2" :  
					Timestamp ts =new Timestamp(System.currentTimeMillis());
					//Add to queue and sort
					if(!IS_LOCKED){
						System.out.println("Locking for Client: " + clientId);
						IS_LOCKED = true;
						LOCKED_CLIENT_ID = clientId;
						System.out.println("Locked for Client: " + LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
						LOCKED_REQ_TS = ts;
						System.out.println("Sending Permit Request");
						for(int quorum : QUORUMS_LIST.get(clientId)){
							System.out.println(quorum);
							if(quorum != clientId){
								Thread getPermission = new SendPermitRequest(clientId,QUORUM_MAP.get(quorum),ts);
								getPermission.setName("ClientReqThread"+quorum);
								getPermission.start();
							}
							
						}

					}
					else{
						addAndSortWaitQueue(ts, clientId);//?? should add in else part?
						System.out.println("Cannot Process Request at the moment");
						System.out.println("Locked for Client : " + LOCKED_CLIENT_ID);
						System.out.println("Request added to the queue and will be processed later");

					}
					
					//LATCH.await();
					System.out.println("Ending  : " + Thread.currentThread().getName());
					break;


				}
				
			}
			
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}


}
/**
 * Requests for Permit from its QUORUM when SELF wants to enter critical section
 */
class SendPermitRequest extends Thread 
{
	private String _serverAddress;
	private int _clientId;
	private Timestamp _timeStamp;

	public SendPermitRequest (int clientId,String serverAdd,Timestamp timestamp)
	{
		_serverAddress = serverAdd;
		_clientId = clientId;
		_timeStamp = timestamp;
	}

	@Override 
	public void run()
	{
		try {
			Socket s = new Socket(_serverAddress,MaikawaClient.CLIENT_PORT);	

			while(true)
			{
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				//Common request type
				System.out.println("Sending Request :" + RQUEST_TYPE.PERMIT);
				String rt =  RQUEST_TYPE.PERMIT;
				System.out.println(rt ==  RQUEST_TYPE.PERMIT);
				dos.writeUTF(RQUEST_TYPE.PERMIT);
				dos.writeInt(_clientId);
				dos.writeUTF(_timeStamp.toString());
				dos.close();
				dis.close();
				s.close();
				break;
			}
				
			System.out.println("Permit Request to Client : " +  MaikawaClient.CLIENT_ID_LIST.get(_serverAddress) + "Sent" );	
			System.out.println("Client : " +  MaikawaClient.CLIENT_ID_LIST.get(_serverAddress) + "disconnected" );

		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		//_cLatch.countDown(); //reduce the count of lactch
	}


}

/**
 * Sends Release to its QUORUM when SELF has finished execution of  critical section
 */
class SendRelease extends Thread 
{
	private String _serverAddress;
	private int _clientId;
	private CountDownLatch _clatch;

	public SendRelease(int clientId, String serveAdd)
	{
		_serverAddress = serveAdd;
		_clientId = clientId;
		_clatch = new CountDownLatch(0);
	}
	public SendRelease(int clientId, String serveAdd,CountDownLatch latch)
	{
		_serverAddress = serveAdd;
		_clientId = clientId;
		_clatch = latch;
	}

	@Override
	public void run(){
		try {
			Socket s = new Socket(_serverAddress,MaikawaClient.CLIENT_PORT);

			while(true){
				System.out.println("Release Invoked");
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(RQUEST_TYPE.RELEASE);//write request type
				dos.writeInt(_clientId);
				dos.close();;
				dis.close();
				s.close();
				break;
			}
		} 
		catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Clatch" + _clatch);
		if(_clatch.getCount()>0){
			_clatch.countDown();
		}
	}
}

/**
 * Sends Yield to the Client that received Inquire when SELF has failed to Lock its QUORUM
 */
class SendYield extends Thread 
{
	private String _serverAddress;
	private int _clientId;
	private CountDownLatch _clatch;

	public SendYield(int clientId, String serveAdd)
	{
		_serverAddress = serveAdd;
		_clientId = clientId;
		_clatch = new CountDownLatch(0);
	}
	public SendYield(int clientId, String serveAdd, CountDownLatch latch)
	{
		_serverAddress = serveAdd;
		_clientId = clientId;
		_clatch = latch;
	}

	@Override
	public void run(){
		try {
			Socket s = new Socket(_serverAddress,MaikawaClient.CLIENT_PORT);

			while(true){
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(RQUEST_TYPE.YIELD);//write request type
				dos.writeInt(_clientId);
				dos.close();;
				dis.close();
				s.close();
				break;
			}
		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Clatch" + _clatch);
		if(_clatch.getCount()>0){
			_clatch.countDown();
		}
	}
}

/**
 * Sends INQUIRE to its CURRENT_LOCKED_CLIENT_ID when SELF has received a higher priority request
 */
class SendInquire extends Thread {
	private String _serverAddress;
	private int _clientId;

	public SendInquire(int clientId, String serveAdd)
	{
		_serverAddress = serveAdd;
		_clientId = clientId;
	}

	@Override
	public void run(){
		try {
			Socket s = new Socket(_serverAddress,MaikawaClient.CLIENT_PORT);

			while(true){
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(RQUEST_TYPE.INQUIRE);//write request type
				dos.writeInt(_clientId);
				dos.close();;
				dis.close();
				s.close();
				break;
			}
		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/**
 * Processes requests in the WAIT_QUEUE(OUTSTANDING REQUESTS)
 * SENDS PERMIT - if WAIT_QUEUE_CLIENT_ID  == SELF
 * SENDS LOCKED - otherwise
 */
class HandleWaitQueue extends Thread {
	private int _clientId;
	private boolean _isFailed = false;

	public HandleWaitQueue(int clientId){
		_clientId = clientId;
	}
	public HandleWaitQueue(int clientId,boolean isFailed){
		_clientId = clientId;
		_isFailed = isFailed;
	}

	synchronized private void ProcessWaitRequest() throws InterruptedException{
		try {
			System.out.println("Sending Reply to outstanding requests");
			Map.Entry<Timestamp,Integer> firstEntry = MaikawaClient.WAIT_QUEUE.entrySet().stream().findFirst().get();
			System.out.println("Outstanding Requets : {");
			for(Timestamp ts : MaikawaClient.WAIT_QUEUE.keySet()){
				System.out.print(MaikawaClient.WAIT_QUEUE.get(ts));
			}
			System.out.print(" }");
			
			String serverAdd = MaikawaClient.QUORUM_MAP.get(firstEntry.getValue());
			if(firstEntry.getValue()== _clientId ){
				if(!_isFailed){
					System.out.println("Removing Value from wait queue");
					MaikawaClient.WAIT_QUEUE.remove(firstEntry.getKey());
					MaikawaClient.IS_LOCKED = true;
					MaikawaClient.LOCKED_CLIENT_ID = _clientId;
					System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
					MaikawaClient.LOCKED_REQ_TS = firstEntry.getKey();
					System.out.println("ProcessWaitRequest: Sending Permit Request");
					for(int quorum : MaikawaClient.QUORUMS_LIST.get(_clientId)){
						System.out.println(quorum);
						if(quorum != _clientId){
							Thread getPermission = new SendPermitRequest(_clientId,MaikawaClient.QUORUM_MAP.get(quorum),firstEntry.getKey());
							getPermission.setName("ClientReqThread"+quorum);
							getPermission.start();
						}
						
					}
				}
				
			}
			else{
				MaikawaClient.IS_LOCKED = true;
				MaikawaClient.LOCKED_CLIENT_ID = firstEntry.getValue();
				System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
				MaikawaClient.LOCKED_REQ_TS = firstEntry.getKey();
				MaikawaClient.WAIT_QUEUE.remove(firstEntry.getKey());
				SendSynchronousRequests.SendLocked(_clientId, serverAdd);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void run(){
		try {
			ProcessWaitRequest();
		} catch(InterruptedException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/**
 * Contains SEENDLOCKED so that it can be accessed synchronously by other threads
 */
class SendSynchronousRequests {
	synchronized public static void SendLocked(int clientId, String serverAdd){
		try {
			Socket s = new Socket(serverAdd,MaikawaClient.CLIENT_PORT);

			while(true){
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(RQUEST_TYPE.LOCKED);//write request type
				dos.writeInt(clientId);
				dos.close();;
				dis.close();
				s.close();
				System.out.println("Locked Request Sent to Client");
				break;
			}
		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}

/**
 * Listend to Icomming QUORUM REQUESTS
 * PERMIT
 * LOCKED
 * FAILED
 * RELEASED
 * YIELD
 * INQUIRE
 * @param clientId
 * @param sClient
 */
class ClientHandler extends Thread{
	int _clientId;
	private final Socket _sclient;
	
	public ClientHandler(int clientId, Socket sClient){
		_clientId = clientId;
		_sclient = sClient;
	}
	@Override
	public void run (){
		try {
			String requestType = null;
			int reqClientId = 999;
			Timestamp timestamp = null;
			while(true){
				try {
					System.out.println("Connected accepted");
					//sClient = _clientSocket.accept();
					DataInputStream dis = new DataInputStream(_sclient.getInputStream());
					DataOutputStream dos = new DataOutputStream(_sclient.getOutputStream());
					requestType = dis.readUTF();
					reqClientId = dis.readInt();
					Thread.currentThread().setName(requestType+reqClientId);

					switch(requestType){
						case RQUEST_TYPE.PERMIT :
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Received : " + requestType + " from Client : " + reqClientId);
						String timeTs = dis.readUTF();
						timestamp = Timestamp.valueOf(timeTs.trim());
						break;
						case RQUEST_TYPE.LOCKED :
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Received : " + requestType + " from Client : " + reqClientId);
						break;
						case RQUEST_TYPE.FAILED :
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Received : " + requestType + " from Client : " + reqClientId);
						break;
						case RQUEST_TYPE.RELEASE :
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Received : " + requestType + "from Client : " + reqClientId);
						break;
						case RQUEST_TYPE.YIELD :
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Received : " + requestType + " from Client : " + reqClientId);
						break;
						case RQUEST_TYPE.INQUIRE :
						System.out.println("Received : " + requestType + " from Client : " + reqClientId);
						break;
						default:
						System.out.print("Thread : " + Thread.currentThread().getName());
						System.out.println("Invalid : " + requestType + " from Client : " + reqClientId);
						

					}
					dos.close();
					dis.close();
					_sclient.close();
					System.out.print("Thread : " + Thread.currentThread().getName());
					System.out.println("Connection closed");
					break;
					
				} catch(IOException i){
					i.printStackTrace();
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				
				
			}
			switch(requestType){
				case RQUEST_TYPE.PERMIT :
				System.out.print("Thread : " + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandlePermitRequest(reqClientId, timestamp);
				break;
				case RQUEST_TYPE.LOCKED :
				System.out.print("Thread : " + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandleLocked(reqClientId);
				break;
				case RQUEST_TYPE.FAILED :
				System.out.print("Thread :" + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandleFailed(reqClientId);
				break;
				case RQUEST_TYPE.RELEASE :
				System.out.print("Thread :" + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandleRelease(reqClientId);
				break;
				case RQUEST_TYPE.YIELD :
				System.out.print("Thread :" + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandleYield(reqClientId);
				break;
				case RQUEST_TYPE.INQUIRE :
				System.out.print("Thread :" + Thread.currentThread().getName());
				System.out.println("Handling : " + requestType + " from Client : " + reqClientId);
				HandleInquire(reqClientId);
				break;
				default:
				System.out.print("Thread :" + Thread.currentThread().getName());
				System.out.println("Invalid : " + requestType + " from Client : " + reqClientId);
				

			}

		
			
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	synchronized private void HandlePermitRequest(int reqClientId, Timestamp timestamp){
		
		if(MaikawaClient.IS_LOCKED){
			//get the current Locked request ts 
			MaikawaClient.addAndSortWaitQueue(timestamp, reqClientId);
			if(timestamp.before(MaikawaClient.LOCKED_REQ_TS)){
				if(MaikawaClient.LOCKED_CLIENT_ID!=_clientId && !MaikawaClient.IS_INQUIRE_SENT){
					String serverAdd = MaikawaClient.QUORUM_MAP.get(MaikawaClient.LOCKED_CLIENT_ID);
					//need not be a thread
					Thread sendInquire = new SendInquire(_clientId,serverAdd);
					MaikawaClient.IS_INQUIRE_SENT = true;
					sendInquire.start();
				}
				

			}
			else{
				System.out.println("Sending failed to : " + reqClientId );
				String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
				SendFailed(serverAdd);
			}		

		}
		else{
			//check if request queue is empty
			// yes--> lock .. No? add request to the queue if not already present
			if(MaikawaClient.WAIT_QUEUE.size()>0){
				Map.Entry<Timestamp,Integer> firstEntry = MaikawaClient.WAIT_QUEUE.entrySet().stream().findFirst().get();
				if(firstEntry.getKey().before(timestamp)){
					System.out.println("Permit for client : " + reqClientId + " cannot be granted");
					if(!MaikawaClient.WAIT_QUEUE.containsKey(timestamp)){
						System.out.println("Adding Client : " + reqClientId + " to the queue");
						MaikawaClient.addAndSortWaitQueue(timestamp, reqClientId);
					}
					System.out.println("Sending failed to : " + reqClientId );
					String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
					SendFailed(serverAdd);
				}
				else{
					System.out.println("Locking resource for Client : " + reqClientId);
					String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
					MaikawaClient.IS_LOCKED = true;
					MaikawaClient.LOCKED_REQ_TS  = timestamp;
					MaikawaClient.LOCKED_CLIENT_ID = reqClientId;
					System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
					SendSynchronousRequests.SendLocked(_clientId,serverAdd);
				}
				
			}
			else{
				System.out.println("Locking resource for Client : " + reqClientId);
				String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
				MaikawaClient.IS_LOCKED = true;
				MaikawaClient.LOCKED_REQ_TS  = timestamp;
				MaikawaClient.LOCKED_CLIENT_ID = reqClientId;
				System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
				SendSynchronousRequests.SendLocked(_clientId,serverAdd);
			}
		}
		

	}
	
	synchronized private void HandleLocked(int reqClientId){
		try {
			System.out.print("Thread : " + Thread.currentThread().getName()+" Running Handle Locked");	
			System.out.println("Is Client " + +_clientId + " locked? : " + MaikawaClient.IS_LOCKED);
			System.out.println("Locked for  " +MaikawaClient.LOCKED_CLIENT_ID);
			if(MaikawaClient.IS_LOCKED){
				if(MaikawaClient.LOCKED_CLIENT_ID == _clientId){
					int currentValue = MaikawaClient.QUORUM_REPLY.get("Locked");
					MaikawaClient.QUORUM_REPLY.replace("Locked", currentValue+1);
		
					int messageCount = MaikawaClient.QUORUM_REPLY.get("Locked") + MaikawaClient.QUORUM_REPLY.get("Failed");
		
					if(messageCount == 2 && MaikawaClient.QUORUM_REPLY.get("Locked")==2){
						//Execute Critical section
						System.out.println("All messages received");
						System.out.println("CS Started");
						//call the three serverrs and enter CS
						CountDownLatch sLatch = new CountDownLatch(1);
						Thread serverThread  = new ServerHandler(_clientId,sLatch);
						serverThread.start();
						sLatch.await();
						if(!MaikawaClient.IS_WRITE_SUCCESS){
							System.out.println("Commit Failed");
							MaikawaClient.addAndSortWaitQueue(MaikawaClient.LOCKED_REQ_TS, MaikawaClient.LOCKED_CLIENT_ID);
						}
						else{
							System.out.println("Commit Successfull");
						}
						//Thread.sleep(10000);
						System.out.println("CS Ended");
						MaikawaClient.LOCKED_REQ_TS  = null;
						MaikawaClient.LOCKED_CLIENT_ID = null;
						System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
						MaikawaClient.IS_LOCKED = false;
						MaikawaClient.QUORUM_REPLY.replace("Locked",0);
						MaikawaClient.QUORUM_REPLY.replace("Failed",0);
						//Remove this request from queue
						//Send release here
						CountDownLatch latch = new CountDownLatch(2);
						for(int quorum : MaikawaClient.QUORUMS_LIST.get(_clientId)){
							if(quorum != _clientId){
								System.out.println(Thread.currentThread().getName() + "Releasing Clients");
								Thread sendRelease = new SendRelease(_clientId,MaikawaClient.QUORUM_MAP.get(quorum),latch);
								sendRelease.start();
							}
						}
						latch.await();
						// wait for release to the sent
						if(MaikawaClient.WAIT_QUEUE.size()>0){
							System.out.println("Thread : " + Thread.currentThread().getName() + " Handling wait requests ");
							Thread handleWaitQueue = new HandleWaitQueue(_clientId);
							handleWaitQueue.start();
						}
		
					}
				}
				else{
					System.out.println("Locking request Expired");
					System.out.println("Locked for Client : " + MaikawaClient.LOCKED_CLIENT_ID);
					System.out.println("Releasing Client : " + reqClientId);
					String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
					Thread sendRelease = new SendRelease(_clientId,serverAdd);
					sendRelease.start();
				}
			}
			else{
				//Check if I am head of the wait queue
				//yes--> lock my self and remove from the queue
				Map.Entry<Timestamp,Integer> firstEntry = MaikawaClient.WAIT_QUEUE.entrySet().stream().findFirst().get();
				if(firstEntry.getValue()== _clientId ){
					System.out.println("Locking Self");
					MaikawaClient.IS_LOCKED = true;
					MaikawaClient.LOCKED_CLIENT_ID = _clientId;
					System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
					MaikawaClient.LOCKED_REQ_TS = firstEntry.getKey();
					MaikawaClient.WAIT_QUEUE.remove(firstEntry.getKey());
					int currentValue = MaikawaClient.QUORUM_REPLY.get("Locked");
					MaikawaClient.QUORUM_REPLY.replace("Locked", currentValue+1);
					for(int quorum : MaikawaClient.QUORUMS_LIST.get(_clientId)){
						System.out.println(quorum);
						if(quorum != _clientId && quorum !=reqClientId){
							Thread getPermission = new SendPermitRequest(_clientId,MaikawaClient.QUORUM_MAP.get(quorum),MaikawaClient.LOCKED_REQ_TS);
							getPermission.setName("ClientReqThread"+quorum);
							getPermission.start();
						}
						
					}
				}
				else{
					System.out.println("Locking request Expired");
					System.out.println("Releasing Client : " + reqClientId);
					String serverAdd = MaikawaClient.QUORUM_MAP.get(reqClientId);
					Thread sendRelease = new SendRelease(_clientId,serverAdd);
					sendRelease.start();
				}
				
			}
			System.out.println("Thread :" + Thread.currentThread().getName() + " EXIT From Handle Locked");
		} 
		catch(InterruptedException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	synchronized private void HandleFailed(int reqClientId){
		try{
			if(MaikawaClient.IS_LOCKED){
				int currentValue = MaikawaClient.QUORUM_REPLY.get("Failed");
				MaikawaClient.QUORUM_REPLY.replace("Failed", currentValue+1);

				if(MaikawaClient.QUORUM_REPLY.get("Failed")>0){
					System.out.println("Processing Failed");
					MaikawaClient.addAndSortWaitQueue(MaikawaClient.LOCKED_REQ_TS, MaikawaClient.LOCKED_CLIENT_ID);
					MaikawaClient.LOCKED_REQ_TS  = null;
					MaikawaClient.LOCKED_CLIENT_ID = null;
					System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
					MaikawaClient.IS_LOCKED = false;
					MaikawaClient.QUORUM_REPLY.replace("Locked",0);
					MaikawaClient.QUORUM_REPLY.replace("Failed",0);

				}
				CountDownLatch latch  = new CountDownLatch(2);
				for(int quorum : MaikawaClient.QUORUMS_LIST.get(_clientId)){
					if(quorum != _clientId){
						System.out.println(Thread.currentThread().getName() + " Releasing Clients");
						Thread send = MaikawaClient.IS_INQUIRE_RECEIVED ? 
						new SendYield(_clientId,MaikawaClient.QUORUM_MAP.get(quorum),latch):
						new SendRelease(_clientId,MaikawaClient.QUORUM_MAP.get(quorum),latch);
						send.start();
					}
				}
				//apply lactching here also
				//Move this part to another class and mark it as synchronised.
				latch.await();
				
				
				if(MaikawaClient.WAIT_QUEUE.size()>0){
					System.out.println("Thread : " + Thread.currentThread().getName() + " Handling wait requests ");
					Thread handleWaitQueue = new HandleWaitQueue(_clientId,true);
					handleWaitQueue.start();

				}
			}
			else{
				System.out.println("Invalid Failed");
			}
			
		}
		catch(InterruptedException i){
			i.printStackTrace();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	synchronized private void HandleRelease(int reqClientId){
		if(MaikawaClient.IS_LOCKED && MaikawaClient.LOCKED_CLIENT_ID==reqClientId){
			System.out.println("LOCKED_CLIENT_ID : " + MaikawaClient.LOCKED_CLIENT_ID + "REQUEST_CLIENT_ID : " + reqClientId);
			if(MaikawaClient.LOCKED_CLIENT_ID!=null && MaikawaClient.LOCKED_CLIENT_ID == reqClientId){
				System.out.println("Release for " + reqClientId + " received" );
				MaikawaClient.IS_INQUIRE_SENT = false;
				MaikawaClient.LOCKED_REQ_TS  = null;
				MaikawaClient.LOCKED_CLIENT_ID = null;
				System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
				MaikawaClient.IS_LOCKED = false;

				if(MaikawaClient.WAIT_QUEUE.size()>0){
					System.out.println("Thread : " + Thread.currentThread().getName() + " Handling wait requests ");
					Thread handleWaitQueue = new HandleWaitQueue(_clientId);
					handleWaitQueue.start();			
				}
			}
			else{
				System.out.println("Invalid Release");
			}
		}
		else{
			System.out.println("Invalid Release");
		}
	}

	synchronized private void HandleYield(int reqClientId){
		if(MaikawaClient.IS_LOCKED){
			if(MaikawaClient.LOCKED_CLIENT_ID == reqClientId){
				MaikawaClient.IS_INQUIRE_SENT = false;
				System.out.println("Yield for " + reqClientId + " received" );
				MaikawaClient.LOCKED_REQ_TS  = null;
				MaikawaClient.LOCKED_CLIENT_ID = null;
				System.out.println("Locked for Client: " + MaikawaClient.LOCKED_CLIENT_ID + "  - by Thread " + Thread.currentThread().getName());
				MaikawaClient.IS_LOCKED = false;
	
				if(MaikawaClient.WAIT_QUEUE.size()>0){
					System.out.println("Thread : " + Thread.currentThread().getName() + " Handling wait requests ");
					Thread handleWaitQueue = new HandleWaitQueue(_clientId);
					handleWaitQueue.start();
		
				}
			}
			else{
				System.out.println("Invalid Yield");
			}
		}
		else{
			System.out.println("Invalid Yield");
		}
	}

	synchronized private void HandleInquire(int reqClientId){
		if(MaikawaClient.IS_LOCKED){
			MaikawaClient.IS_INQUIRE_RECEIVED = true;
		}
		else{
			System.out.println("Invalid Inquire");
		}
	}

	synchronized private void SendFailed(String serverAdd){
		try {
			Socket s = new Socket(serverAdd,MaikawaClient.CLIENT_PORT);

			while(true){
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeUTF(RQUEST_TYPE.FAILED);//write request type
				dos.writeInt(_clientId);
				dos.close();;
				dis.close();
				s.close();
				break;
			}
		} catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}


}

/**
 * Connects to Server_PORT
 * @param clientId
 * @param latch
 */

class ServerHandler extends Thread{
	int _clientId;
	private static CountDownLatch _latch;
	public static List<String> ACTIVE_SERVERS_LIST = new ArrayList<String>(Arrays.asList(
	"dc01.utdallas.edu",
	"dc02.utdallas.edu",
	"dc03.utdallas.edu")); 
	
	public ServerHandler (int clientId , CountDownLatch latch){
		_clientId = clientId;
		_latch = latch;
		READY_COUNT =0;
		ABORT_COUNT = 0;
		MESSAGE_COUNT = 0;
	}

	public static volatile int READY_COUNT;
	public static volatile int ABORT_COUNT;
	public static volatile int MESSAGE_COUNT;
	
	@Override 
	public void run(){
		try {
			System.out.println("Running on Server Handler");
			CountDownLatch sLatch = new CountDownLatch(3);
			CountDownLatch notifyLatch = new CountDownLatch(3);
			for(String server : ACTIVE_SERVERS_LIST){
				Thread twoPhaseServer = new TwoPhaseCommitProtocol(_clientId,server,sLatch,notifyLatch);
				twoPhaseServer.start();
			}
			sLatch.await();
			System.out.println("Server Handler Completed : Setting variables to 0");
			READY_COUNT =0;
			ABORT_COUNT = 0;
			MESSAGE_COUNT = 0;
			System.out.println("Variables Set");
		} catch (Exception e) {
			//TODO: handle exception
		}
		_latch.countDown();
	}
}

/**
 * Listens to Incomming Requests
 * Spawns a new thread for each request
 * @param clientId
 */
class ClientListener extends Thread
{
	int _clientId;
	
	public ClientListener(int clientId){
		_clientId = clientId;
		
	}
	@Override
	public void run(){
		try {
			ServerSocket clientSocket = new ServerSocket(MaikawaClient.CLIENT_PORT);
			
			while(true){
				Socket sClient = null;	
				try {
					sClient = clientSocket.accept();
					System.out.println("Request Received :  Starting new thread for Hanling");
					Thread clientHandleThread = new ClientHandler(_clientId,sClient);
					clientHandleThread.start();
				} 
				catch(IOException i){
					clientSocket.close();
					i.printStackTrace();
				}
				catch (Exception e) {
					clientSocket.close();
					e.printStackTrace();
				}
			}
			
		} 
		catch(IOException i){
			i.printStackTrace();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

/**
 * Implements Two phase commit protocol
 * Phase 1-- Sends Prepare message to all the servers in the Cohort
 * Phase 2-- Commits -- if all servers send Ready , Aborts -- if any one server aborts
 * @param clientId
 * @param serverName
 * @param latch
 * @param notifyLatch
 */

class TwoPhaseCommitProtocol extends Thread{
	int _clientId;
	private static CountDownLatch _latch;
	private static CountDownLatch _notifyLatch;
	private String _serverName;

	
	public TwoPhaseCommitProtocol(int clientId ,String serverName, CountDownLatch latch, CountDownLatch notifyLatch){
		_clientId = clientId;
		_latch = latch;
		_serverName = serverName;
	}
	synchronized private void PerformSynchronousWrite(){
		try {
			Socket s = new Socket(_serverName,MaikawaClient.SERVER_PORT);
			while(true){
				System.out.println("Sending Request to server");
				DataInputStream dis = new DataInputStream(s.getInputStream());
				DataOutputStream dos = new DataOutputStream(s.getOutputStream());
				dos.writeInt(_clientId);
				dos.writeUTF("PREPARE");//write request type
				Boolean sReply  =  dis.readBoolean();
				if(sReply){//implies commit
					System.out.println("Ready received from : " + _serverName);
					ServerHandler.READY_COUNT ++;
					ServerHandler.MESSAGE_COUNT++;
					//_notifyLatch.countDown();
				}
				else{
					System.out.println("Abort received from : " + _serverName);
					ServerHandler.ABORT_COUNT++;
					ServerHandler.MESSAGE_COUNT++;
					//_notifyLatch.countDown();
				}
				while(ServerHandler.MESSAGE_COUNT !=3){
					System.out.println("Message Count : " + ServerHandler.MESSAGE_COUNT);
					if(ServerHandler.MESSAGE_COUNT == 3){
						System.out.println("All  : " + ServerHandler.MESSAGE_COUNT + " Received");
						break;
					}
				}
				//System.out.println("Notify lactch Count"+_notifyLatch.getCount());
				//_notifyLatch.await();
				if(ServerHandler.ABORT_COUNT == 0 && ServerHandler.READY_COUNT == 3){
					dos.writeBoolean(true);
					String message = "Client ID :  " + MaikawaClient.LOCKED_CLIENT_ID + " , Timestamp :  " + MaikawaClient.LOCKED_REQ_TS + " \n" ;
					dos.writeUTF(message);
					MaikawaClient.IS_WRITE_SUCCESS = dis.readBoolean();
				}
				else{
					dos.writeBoolean(false);
					MaikawaClient.IS_WRITE_SUCCESS = false;
				}	
				dos.close();;
				dis.close();
				s.close();
				break;
			}
			System.out.println("Recived server reply");
		} catch(EOFException e){
			e.printStackTrace();
			

		}
		catch(IOException i)
		{
			i.printStackTrace();
			
		}
		catch(Exception e){

			e.printStackTrace();
			
		}
	}
	@Override 
	public void run(){
		try {
			PerformSynchronousWrite();
		} catch (Exception e) {
			//TODO: handle exception
		}
		_latch.countDown();
	}
}