// Scripts related to the Snapshot Node

package main

import (
	"os"
	"strings"
	"strconv"
	"context"
	"time"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	pb "gocode/chatpb"
)


type NodeContacts struct {
	Name string
	Port string
	LeaderName string
	LeaderPort string
	CNodes []string
	State string
}

//------------------------------------------------------------------------------------------------------------------------------
// Global variables & global constants
const snapPeriod = 100	//expressed in seconds
var thisNode NodeContacts
var everyElTime [][]time.Duration
//------------------------------------------------------------------------------------------------------------------------------
func setInfo(){
	// Memorize the node's information
	thisNode.Name = os.Getenv("NAME")
	thisNode.Port = os.Getenv("PORT")

	// Memorize names and ports of the other nodes (docker-compose hardcoded version)
	value := os.Getenv("ALLCNODES")
	if value == "" {
		customPrintln("ALLCNODES environment variable is empty")
	}else{
		thisNode.CNodes = strings.Split(value, ",")
	}
}
//------------------------------------------------------------------------------------------------------------------------------
func recoverElectionTime(){
	var everyRow string

	// Reset variable
	for i := range everyElTime {
    		everyElTime[i] = everyElTime[i][:0]
	}

	// For every Consensus Node
	for _, CNodeInfo := range thisNode.CNodes {

		// Create the connection
		conn, err := grpc.Dial(CNodeInfo,grpc.WithInsecure(),)

		// In case of errors with Dial
		if err != nil {
			customPrintln("DIAL ERROR - Tring with the next node")
			continue //try the next cnode
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "Leadership"
		client := pb.NewReadingWritingClient(conn)

		// Send the message and obtain the Reply
		repliedRows, err := client.GetAllLog(context.Background(),&emptypb.Empty{},)
		// In case of errors with RPC
		if err != nil {
			customPrintln("RPC ERROR - Tring with the next node")
			continue //try the next cnode
		// All went fine with RPC
		}else{
			everyRow = repliedRows.Status
			// Check if the received string is not empty
			if(everyRow != ""){
				for _, row := range strings.Split(everyRow, "\n") {
					if strings.HasPrefix(row, "[ELECTION-TIME]"){
						// Extract the time value between < and > and convert it
						timeStr := strings.Split(strings.Split(row, "<")[1], ">")[0]
						duration, _ := time.ParseDuration(timeStr)

						// Extract the number of node
						nodeStr := strings.TrimSuffix(strings.TrimSpace(strings.Split(row, ":")[1]), ")")
						NNode, err := strconv.Atoi(nodeStr)
						if err != nil {
							panic(err)
						}

						// Add duration to the corresponding slice
						if(NNode>0 && NNode<=15){
							everyElTime[NNode-1] = append(everyElTime[NNode-1], duration)
						}
					}
				}
			}
		}
	}
}


func computeStatElecTime(){
	customPrintln("EMPIRICAL DATA")
	
	var sum time.Duration = 0
	var min time.Duration = 1 * time.Hour
	var max time.Duration = 0

	for i := 0; i < len(everyElTime); i++ {
		if(len(everyElTime[i]) == 0){
			fmt.Println("[",i+1," nodes] Not enough data for this number of nodes")
		}else{
			sum = 0
			for _, elem := range everyElTime[i]{
					sum += elem
					if(elem > max){
						max = elem
					}
					if(elem < min){
						min = elem
					}
					
			}
	
			mean := sum / time.Duration(len(everyElTime[i]))
			fmt.Println("[",i+1, "nodes] Mean election time = ",mean,"min Value = ",min," max value = ",max," - (",len(everyElTime[i])," samples)")
			
		}
	}
}


func recoverOperationLog(){
	var everyRow string

	customPrintln("RESUME OF EVERY OPERATION")
	for _, CNodeInfo := range thisNode.CNodes {
		// Create the connection
		conn, err := grpc.Dial(CNodeInfo,grpc.WithInsecure(),)

		// In case of errors with Dial
		if err != nil {
			customPrintln("DIAL ERROR - Tring with another node")
			continue //try the next cnode
		}

		// Pospone the closing
		defer conn.Close()

		// Create the client for the service "Leadership"
		client := pb.NewReadingWritingClient(conn)

		// Send the message and obtain the Reply
		repliedRows, err := client.GetAllLog(context.Background(),&emptypb.Empty{},)
		// In case of errors with RPC
		if err != nil {
			customPrintln("RPC ERROR")
			continue //try the next cnode
		// All went fine with RPC
		}else{
			everyRow = repliedRows.Status
			// Check if the received string is not empty
			if(everyRow != ""){
				for _, row := range strings.Split(everyRow, "\n") {
					if strings.HasPrefix(row, "[ADDPAIR]"){
						fmt.Println("["+CNodeInfo+"] ROW: " + row)
					}
				}
			}
		}
	}
}


//------------------------------------------------------------------------------------------------------------------------------
func main() {
	//Print a short welcoming message at the start
	greetings()

	//Initial setup
	setInfo()

	// Start the listener using a goroutine
	go startServer(thisNode.Name, thisNode.Port)

	// Setup the Data Collector (docker-compose hardcoded version)
	everyElTime = make([][]time.Duration, 15)

	//Interrogate periodically the consensus nodes
	for{
		time.Sleep(snapPeriod * time.Second)
		recoverElectionTime()
		computeStatElecTime()
		recoverOperationLog()
	}
}
