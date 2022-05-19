main:
	@echo "Follow below steps"
	@echo "Step 1. Run the command make setFileContents on ANY ONE SERVER to clear contents of all Files"
	@echo "Step 2. Run the command make server for starting the servers"
	@echo "Step 3. Run the command make client for starting the client"
	@echo "Step 4. Run the command make clean on either servers"
setFileContents:
	cd Server0
	>F1.txt
	>F2.txt
	cd Server1
	>F1.txt
	>F2.txt
	cd Server2
	>F1.txt
	>F2.txt
	
server:
	javac TCPServer.java
	java TCPServer
client:
	javac Client.java
	java Client
clean:
	rm -f *.class
	
