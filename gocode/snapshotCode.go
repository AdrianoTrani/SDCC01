// Scripts related to the Snapshot Node

package main

import (
	"os"
	"strings"
	"context"
	"time"

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
}
//------------------------------------------------------------------------------------------------------------------------------
// Global variables & global constants
const snapPeriod = 100	//expressed in seconds
var thisNode NodeContacts
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

	customPrintln("--------- RESUME OF EVERY ELECTION TIMES INSIDE EVERY VOLUME ---------")
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
					if strings.HasPrefix(row, "[ELECTION-TIME]"){
						customPrintln("ROW: " + row)
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

	//Interrogate periodically the consensus nodes
	for{
		time.Sleep(snapPeriod * time.Second)
		recoverElectionTime()
	}
}
