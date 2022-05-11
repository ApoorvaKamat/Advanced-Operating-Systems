main:
	@echo "Follow below steps"
	@echo "Step 1. Run the command make setFileContents on ANY ONE SERVER to clear contents of all Files"
	@echo "Step 2. Run the command make build for building Client and server class"
	@echo "Step 3. Run the command make server for starting the servers"
	@echo "Step 4. Run the command make client for starting the client"
	@echo "Step 5. Run the command make clean on either servers"
	@echo ""
	@echo ""
	@echo ""
	@echo "Clients and Servers are fixed for this Project"
	@echo "Servers : "
	@echo "dc01.utdallas.edu"
	@echo "dc02.utdallas.edu"
	@echo "dc03.utdallas.edu"
	@echo ""
	@echo "Clients : "
	@echo "dc30.utdallas.edu"
	@echo "dc31.utdallas.edu"
	@echo "dc32.utdallas.edu"
	@echo "dc33.utdallas.edu"
	@echo "dc34.utdallas.edu"
	
setFileContents:
	cd Server0;> F1.txt;cd ..
	cd Server1;> F1.txt;cd ..
	cd Server2;> F1.txt;cd ..

server:
	java TCPServer
client:
	java MaikawaClient
clean:
	rm -f *.class
build:
	rm -f *.class
	javac TCPServer.java
	javac MaikawaClient.java
	
