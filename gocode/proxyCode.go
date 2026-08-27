// Scripts related to the Client Proxy Service

package main

import (
	//"github.com/redis/go-redis/v9"
	"os"
	"context"
	"errors"
	"time"
	"strings"

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

	/*
	if err != nil {
		return "" , "", errors.New("[RPC ERROR]")
	}
	*/

	//thisNode.LeaderName, thisNode.LeaderPort = "cNode01", "50051"
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
func main() {
	//Print a short welcoming message at the start
	greetings()


	
	// Set personal information
	setInfo()

	// Start the listener using a goroutine
	go startServer(thisNode.Name, thisNode.Port)


	// TEMP Wait
	shortSleep()


	// Quick tests
	var test_result string
	var test_error error
	

	test_result, test_error = writeFromExt("Yoda" , "Uses the Lightsaber")
	if(test_error != nil){
		customPrintln("Something went wrong")
	}else{
		customPrintln("Received a writing request from the outside ----- " + test_result)
	}



	test_result, test_error = writeFromExt("Luke Skywalker" , "Hates sand")
	if(test_error != nil){
		customPrintln("Something went wrong")
	}else{
		customPrintln("Received a writing request from the outside ----- " + test_result)
	}




	test_result, test_error = writeFromExt("Obi Wan Kenobi" , "It's a Jedi Master")
	if(test_error != nil){
		customPrintln("Something went wrong")
	}else{
		customPrintln("Received a writing request from the outside ----- " + test_result)
	}








	// Quick test - LOOP - try to stop leader container using Docker Desktop while you run this
	var res6 string
	var err6 error
	for{
			res6,err6 = readFromExt("Yoda")
			if(err6 != nil){
				customPrintln("Something went wrong:" + err6.Error())

				// Activate CircuitBreaker if not already present
				if !thisNode.CircuitBreaker{go circuitBreaker()}	
			}else{
				customPrintln("Received a reading request from the outside ----- " + res6)
			}
			shortSleep()
	}


	



	// Force the container to stay active
	for{
		//shortSleep()
	}




	/*
	// Quick test
	res1,err1 := readFromExt("Obi Wan Kenobi")
	if(err1 != nil){
		customPrintln("Something went wrong")
	}else{
		customPrintln("Result ----- " + res1)
	}

	// Quick test
	res2,err2 := writeFromExt("Luke Skywalker" , "NOT a Jedi Master")
	customPrintln("Received a writing request from the outside ----- " + res2)

	// Quick test
	res3,err3 := writeFromExt("Obi Wan Kenobi" , "It's a Jedi Master")
	customPrintln("Received a writing request from the outside ----- " + res3)

	// Quick test
	res4,err4 := writeFromExt("Yoda" , "Uses the Lightsaber")
	customPrintln("Received a writing request from the outside ----- " + res4)
	*/


	/*
	res5,err5 := writeFromExt("Yoda" , "Uses the Lightsaber")
	if(err5 != nil){
		
	}else{
		customPrintln("Received a writing request from the outside ----- " + res5)
	}
	*/



}