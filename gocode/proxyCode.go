// Scripts related to the Client Proxy Service

package main

import (
	"os"
	"context"
	"errors"
	"time"
	"strings"
	"bufio"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	pb "gocode/chatpb"
	
)


type NodeInfo struct {
	Name string
	Port string
	CircuitBreaker bool
	LeaderName string
	LeaderPort string
	CNodes []string
}


// Global Constants & Variables
var thisNode NodeInfo
const circuitBreakerTimer = 60	//expressed in seconds
//------------------------------------------------------------------------------------------------------------------------------
func whoIsLeader(){
	customPrintln("Client Proxy wants to know who is the leader")

	for _, CNodeInfo := range thisNode.CNodes {
		// Create the connection
		//conn, err := grpc.Dial(thisNode.LeaderName+":"+thisNode.LeaderPort,grpc.WithInsecure(),grpc.WithBlock(),)
		customPrintln("Asking who is the leader to " + CNodeInfo)
		conn, err := grpc.Dial(CNodeInfo,grpc.WithInsecure(),)


		// In case of errors with Dial
		if err != nil {
			customPrintln("DIAL ERROR - Tring with another node")
			continue //try the next cnode
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "Leadership"
		client := pb.NewLeadershipClient(conn)

		// Send the message and obtain the Reply
		leaderInfo, err := client.LeaderInfo(context.Background(),&emptypb.Empty{},)

		// In case of errors with RPC
		if err != nil {
			customPrintln("RPC ERROR")
			continue //try the next cnode
		}else{
			thisNode.LeaderName = leaderInfo.Leadername
			thisNode.LeaderPort = leaderInfo.Leaderport
			customPrintln("Received: " + thisNode.LeaderName + ":" + thisNode.LeaderPort)
			break //stop asking
		}
	}
}
//------------------------------------------------------------------------------------------------------------------------------
func setInfo(){
	// Memorize the node's information
	thisNode.Name = os.Getenv("NAME")
	thisNode.Port = os.Getenv("PORT")
	thisNode.CircuitBreaker = false


	// Memorize names and ports of the other nodes (docker-compose hardcoded version)
	value := os.Getenv("ALLCNODES")
	if value == "" {
		customPrintln("ALLCNODES environment variable is empty")
	}else{
		thisNode.CNodes = strings.Split(value, ",")
	}

}
//------------------------------------------------------------------------------------------------------------------------------
func circuitBreaker(){
	customPrintln("Time to activate Circuit Breaker")
	thisNode.CircuitBreaker = true
	time.Sleep(circuitBreakerTimer * time.Second)
	thisNode.CircuitBreaker = false
}
//------------------------------------------------------------------------------------------------------------------------------
//Given a key, return the value (if present)
func readFromExt(key string) (string , error) {
	customPrintln("Received a reading request from the outside")

	// If Circuit Breaker is active, do not forward the message
	if(thisNode.CircuitBreaker){
		return "", errors.New("[CIRCUIT BREAKER]")
	}else{
		// Obtain Leader information
		whoIsLeader()

		// Create the connection
		//conn, err := grpc.Dial(thisNode.LeaderName+":"+thisNode.LeaderPort,grpc.WithInsecure(),grpc.WithBlock(),)
		conn, err := grpc.Dial(thisNode.LeaderName+":"+thisNode.LeaderPort,grpc.WithInsecure(),)

		// In case of errors with Dial, return a message with "error" to the caller
		if err != nil {
			return "", errors.New("[DIAL ERROR]")
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "ReadingWriting"
		client := pb.NewReadingWritingClient(conn)
		customPrintln("ReadingWritingClient created")

		// Send the message and obtain the Reply
		reply, err := client.SearchKey(context.Background(),&pb.Message{From: thisNode.Name+":"+thisNode.Port,Text: key,},)
		if err != nil {
			return "" , errors.New("[RPC ERROR]")
		}

		// Return the Reply
		return reply.Status,nil
	}

}

//Given a pair (key,value), insert it in the datastore (if not present)
func writeFromExt(key, value string) (string , error){
	customPrintln("Received a writing request from the outside")

	// If Circuit Breaker is active, do not forward the message
	if(thisNode.CircuitBreaker){
		return "", errors.New("[CIRCUIT BREAKER]")
	}else{
		// Obtain Leader information
		whoIsLeader()

		// Create the connection
		//conn, err := grpc.Dial(thisNode.LeaderName+":"+thisNode.LeaderPort,grpc.WithInsecure(),grpc.WithBlock(),)
		conn, err := grpc.Dial(thisNode.LeaderName+":"+thisNode.LeaderPort,grpc.WithInsecure(),)

		// In case of errors with Dial, return a message with "error" to the caller
		if err != nil {
			return "", errors.New("[DIAL ERROR]")
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "ReadingWriting"
		client := pb.NewReadingWritingClient(conn)
		customPrintln("ReadingWritingClient created")

		// Send the message and obtain the Reply
		reply, err := client.AddPair(context.Background(),&pb.PairKeyValue{Key: key,Value: value,},)
		if err != nil {
			return "" , errors.New("[RPC ERROR]")
		}

		// Return the Value
		if(reply.Status == ""){
			return "Pair not found",nil
		}else{
			return reply.Status,nil
		}
	}
}

//------------------------------------------------------------------------------------------------------------------------------
func handleConnection(conn net.Conn) {
	// Variables
	myScanner := bufio.NewScanner(conn)
	var command_result string
	var command_error error

	// Pospone closing
	defer conn.Close()

	// Brief introduction
	fmt.Fprintf(conn , "Client Proxy Console - Instructions\n")
	fmt.Fprintf(conn , "SEARCH:<key>\n")
	fmt.Fprintf(conn , "INSERT:<key>,<value>\n")
	fmt.Fprintf(conn , "COMMAND> ")

	// Input cycle - one command at a time
	for myScanner.Scan(){
		// Check for errors
		if err := myScanner.Err(); err != nil {
        		log.Println("connection:", err)
			fmt.Printf("Scanner failed\n")
    		}
		
		//fmt.Fprintf(conn , "Received: %s\n", receivedCommand)

		// Clean the received input and act accordingly
		// 	TrimSpace removes unnecessary spaces
		// 	ToLower makes the console non-case sensitive
		//	SplitN separates command and parameters (if any)
		receivedCommand := strings.TrimSpace(myScanner.Text())
		commandParts := strings.SplitN(receivedCommand, ":", 2)
		commandOnly := commandParts[0]
		parameters := commandParts[1]

		switch strings.ToLower(commandOnly){
			case "SEARCH":
				customPrintln("Received command: " + commandOnly + ":" + parameters)
				command_result, command_error = readFromExt(parameters)
				if(command_error != nil){
					fmt.Fprintf(conn , "Something went wrong: " + command_error.Error() + "\n")
					// Activate CircuitBreaker if not already present
					if !thisNode.CircuitBreaker{go circuitBreaker()}
				}else{
					fmt.Fprintf(conn , "(Key,Value) = (" + parameters + "," + command_result + ")\n")
				}
			case "INSERT":
				/*
				parametersList := strings.SplitN(parameters, ",", 2)
				customPrintln("Received command: " + commandOnly + "(" + parametersList[0] + ","+ parametersList[1] + ")")
				command_result, command_error = writeFromExt(parameters[0],parameters[1])
				if(command_error != nil){
					fmt.Fprintf(conn , "Something went wrong: " + command_error.Error() + "\n")
					// Activate CircuitBreaker if not already present
					if !thisNode.CircuitBreaker{go circuitBreaker()}
				}else{
					fmt.Fprintf(conn , command_result)
				}
				*/
			default:
				customPrintln("Received command: " + commandOnly)
				fmt.Println("> This is NOT a valid command")
		}
		fmt.Fprintf(conn , "COMMAND> ")
	}


}

func startConsoleServer(address string){
	// Start to listen
	listenerConsole, err := net.Listen("tcp", address)
	if err != nil{
		log.Printf("Listen failed\n")
	}

	// Pospone
	defer listenerConsole.Close()

	// Debug print
	log.Printf("TCP input listening on %s", address)

	for {
		conn, err := listenerConsole.Accept()
		if err != nil {
			log.Printf("Accept failed\n")
			continue
        }

        go handleConnection(conn)
    }
}

//------------------------------------------------------------------------------------------------------------------------------
func main() {
	//Print a short welcoming message at the start
	greetings()

	// Set personal information
	setInfo()

	// Wait
	shortSleep()

	// Start the listener using a goroutine
	go startServer(thisNode.Name, thisNode.Port)

	// Start the terminal listener using a goroutine
	go startConsoleServer(":9001")
	
	// Force the container to stay active - unlike an empty for this is more resource-friendly
	select {}
}