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
	State string
}


// Global Constants & Variables
var thisNode NodeInfo
const circuitBreakerTimer = 60	//expressed in seconds
//------------------------------------------------------------------------------------------------------------------------------
func whoIsLeader(){
	for _, CNodeInfo := range thisNode.CNodes {
		// Create the connection
		conn, err := grpc.Dial(CNodeInfo,grpc.WithInsecure(),)

		// In case of errors with Dial
		if err != nil {
			//customPrintln("DIAL ERROR - Tring with another node")
			continue //try to ask to the next cnode
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "Leadership"
		client := pb.NewLeadershipClient(conn)

		// Send the message and obtain the Reply
		leaderInfo, err := client.LeaderInfo(context.Background(),&emptypb.Empty{},)

		// In case of errors with RPC
		if err != nil {
			//customPrintln("RPC ERROR")
			continue //try to ask to the next cnode
		}else{
			thisNode.LeaderName = leaderInfo.Leadername
			thisNode.LeaderPort = leaderInfo.Leaderport

			break //stop asking
			// in case Proxy obtains an answer but there is no leader
			// 	leaderInfo will be empty
			// 	every rpc called with the empty address as target will fail
			// 	circuit breaker will be activated
			// 	in the meantime the consensus nodes will elect a new leader
			//	when there will be a new leader, client proxy will be able to contact it
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
	thisNode.CircuitBreaker = true
	time.Sleep(circuitBreakerTimer * time.Second)
	thisNode.CircuitBreaker = false
}
//------------------------------------------------------------------------------------------------------------------------------
//Given a key, return the value (if present)
func readFromExt(key string) (string , error) {
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
//------------------------------------------------------------------------------------------------------------------------------
func myTerminal() {
	// Variables
	myScanner := bufio.NewScanner(os.Stdin)
	var command_result string
	var command_error error

	// Brief introduction
	fmt.Println("Client Proxy Console - Instructions")
	fmt.Println("SEARCH:<key>")
	fmt.Println("INSERT:<key>,<value>")
	fmt.Println("<COMMAND>")

	// Input cycle - one command at a time
	for myScanner.Scan(){
		// Check for errors
		if err := myScanner.Err(); err != nil {
			fmt.Println("Scanner failed")
    		}
		
		// Make sure the input structure is correct
		receivedCommand := strings.TrimSpace(myScanner.Text())
		if strings.Contains(receivedCommand, ":"){
			// Clean the received input and act accordingly
			//	Contains checks the s
			// 	TrimSpace removes unnecessary spaces
			// 	ToUpper makes the console non-case sensitive by rewriting the command in uppercase
			//	SplitN separates command and parameters (if any)
			commandParts := strings.SplitN(receivedCommand, ":", 2)
			commandOnly := commandParts[0]
			parameters := commandParts[1]

			switch strings.ToUpper(commandOnly){
				case "SEARCH":
					// Check the provided key
					if(parameters == ""){
						fmt.Println("Empty key is not a valid key")
					}else{
						// Execute reading operation
						command_result, command_error = readFromExt(parameters)
		
						// If the reading operation didn't go well
						if(command_error != nil){
							// Print error message and activate CircuitBreaker if not already present
							fmt.Println("Something went wrong: " + command_error.Error())
							if !thisNode.CircuitBreaker{go circuitBreaker()}

						// The reading operation went smoothly
						}else{
							// The requested key doesn't exist
							if(command_result == ""){
								fmt.Println("The key " + parameters + " is not present")

							// Print the pair key-value
							}else{
								fmt.Println("(Key,Value) = (" + parameters + "," + command_result + ")")
							}
						}
					}
				case "INSERT":
					// Make sure the input structure is correct
					if strings.Contains(parameters, ","){
						// Obtain key and value from input
						parametersList := strings.SplitN(parameters, ",", 2)

						// Check the provided key
						if(parametersList[0] == ""){
							fmt.Println("Empty key is not a valid key")
						}else{

							// Execute Writing operation
							command_result, command_error = writeFromExt(string(parametersList[0]),string(parametersList[1]))

							// If the writing operation didn't go well
							if(command_error != nil){
								// Print error message and activate CircuitBreaker if not already present
								fmt.Println("Something went wrong: " + command_error.Error())
								if !thisNode.CircuitBreaker{go circuitBreaker()}

							// The writing operation went smoothly
							}else{
								fmt.Println(command_result)
							}
						}

					// Invalid input structure
					}else{
						fmt.Println("> The provided command is incorrectly written. Please check the correct syntax")
					}


				default:
					fmt.Println("> " + commandOnly + " is NOT a valid command")
			}

		// Input doesn't have the correct structure
		}else{
			fmt.Println("> The provided command is incorrectly written. Please check the correct syntax")
		}



		fmt.Println("<COMMAND>")
	}
}



//------------------------------------------------------------------------------------------------------------------------------
func main() {
	// Set personal information
	setInfo()

	// Start the listener using a goroutine
	go startServer(thisNode.Name, thisNode.Port)

	// Wait
	shortSleep()

	// Start the terminal listener using a goroutine
	go myTerminal()
	
	// Force the container to stay active - unlike an empty for this is more resource-friendly
	select {}
}