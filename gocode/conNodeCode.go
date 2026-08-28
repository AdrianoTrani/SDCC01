// Scripts related to the Consensus Nodes

package main

import (
	"fmt"
	"os"
	"strings"
	"time"
	"sync"
	"context"
	"log"
    	"math/rand"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "gocode/chatpb"
)

type HeartbeatMonitor struct {
	mu    sync.Mutex
	timer *time.Timer
}


type NodeState struct {
	Name string
	Port string
	Peers []string
	LeaderName string
	LeaderPort string
	State string
	RaftTerm uint32
	AlreadyVoted bool
	HBMonitor *HeartbeatMonitor
	
	//IsThereALeader bool
}




//------------------------------------------------------------------------------------------------------------------------------
// Global variables & global constants
var thisNode NodeState
const heartbeatPeriod = 30	//expressed in seconds
const electionTimeout = 80	//expressed in seconds
const restartElection = 60	//expressed in seconds
var convergeTime time.Time	//used to measure the time between the start of an election and the confirmation of a leader
//------------------------------------------------------------------------------------------------------------------------------
// Set the initial values of the struct
func setUpNodeInfo(){
	// Memorize the node's information
	thisNode.Name = os.Getenv("NAME")
	thisNode.Port = os.Getenv("PORT")
	thisNode.State = "FOLLOWER"
	thisNode.RaftTerm = 0
	thisNode.AlreadyVoted = false

	// Memorize names and ports of the other nodes (docker-compose hardcoded version)
	value := os.Getenv("ALLPEERS")
	if value == "" {
		customPrintln("ALLPEERS environment variable is empty")
	}else{
		thisNode.Peers = strings.Split(value, ",")
	}
}
//------------------------------------------------------------------------------------------------------------------------------

//------------------------------------------------------------------------------------------------------------------------------
// Set the selected node as a leader
func setTheLeader(leaderName,leaderPort  string){
	customPrintln("The node "+ leaderName + ":" + leaderPort + " is now the leader")


	thisNode.AlreadyVoted = false
	thisNode.LeaderName = leaderName
	thisNode.LeaderPort = leaderPort

	// Change state
	if(thisNode.Name == leaderName && thisNode.Port == leaderPort){
		thisNode.State = "LEADER"
	}else{
		thisNode.State = "FOLLOWER"
	}
}
//------------------------------------------------------------------------------------------------------------------------------
// Implement the homonym service inside sdcc.proto
func (sl *serverL) RequestVote(ctx context.Context, msg *pb.RaftMessage) (*pb.Reply, error){
	// The sender may be a good candidate for be leader
	if(msg.Term >= thisNode.RaftTerm){
		// The receiver node already voted (for another node or itself) 
		if(thisNode.AlreadyVoted){
			// The receiver node already voted for another node
			if(thisNode.State == "FOLLOWER"){
				customPrintln("This node voted NO to " + msg.From + " (Already voted for another)")
				return &pb.Reply{Status: "FALSE",}, nil
			}

			// The receiver node already voted for itself	
			if(thisNode.State == "CANDIDATE"){
				customPrintln("This node voted YES to " + msg.From + " (Even if already voted for itself)")
				thisNode.State = "FOLLOWER"
				return &pb.Reply{Status: "TRUE",}, nil
			}

			// EXTRA - The receiver node is the leader
			if(thisNode.State == "LEADER"){
				customPrintln("This node voted NO to " + msg.From + " (Because it is the leader)")
				return &pb.Reply{Status: "FALSE3",}, nil
			}
	
		// The receiver never voted
		}else{
			// The receiver is already the leader
			if(thisNode.State == "LEADER"){
				customPrintln("This node voted NO to " + msg.From + " (Because it is already the leader)")
				return &pb.Reply{Status: "FALSE3",}, nil				
			// The receiver never voted and it's a follower
			}else{
				thisNode.AlreadyVoted = true
				thisNode.State = "FOLLOWER"		//A candidate should not be able to enter this branch, put this node as follower nonetheless
				customPrintln("This node voted YES to " + msg.From + " (Because it never voted before)")
				return &pb.Reply{Status: "TRUE",}, nil
			}
		}
	// According to Raft term, the sender is not suitable to be leader
	}else{
		customPrintln("This node voted NO to " + msg.From + " (Inferior Raft Term)")
		return &pb.Reply{Status: "FALSE",}, nil
	}

	// In any other case return special negative vote to the Sender as a way to inform that something went wrong
	return &pb.Reply{Status: "FALSE2",}, nil	
}

// Using Service "Leadership"
// Send a RequestVote rpc to a node
// Return the Reply to the caller/sender
func requestVote(from string, target string, term uint32) string{
	customPrintln("Sending RequestVote to " + target)
	//conn, err := grpc.Dial(target,grpc.WithInsecure(),grpc.WithBlock(),)
	conn, err := grpc.Dial(target,grpc.WithInsecure(),)


	if err != nil {
		log.Println(err)
		return "error"
	}
	defer conn.Close()

	client := pb.NewLeadershipClient(conn)

	reply, err := client.RequestVote(context.Background(),&pb.RaftMessage{From: from,Term: uint32(term),},)
	if err != nil {
		log.Println(err)
		return "error"
	}
	return reply.Status
}

// Check the received votes
// If the majority are "TRUE" then return true
// If the majority are "FALSE" then return false
func majorityChecker(votes []string) (bool){
	var positiveVotes int = 1	// The node voted for itself
	var negativeVotes int = 0
	var notvalidVotes int = 0

	for _, vote := range votes {
		if(vote == "FALSE3"){
			return false
		}
		if(vote == "TRUE"){
			positiveVotes++
		}else{
			if(vote == "FALSE" || vote == "FALSE2"){
				negativeVotes++
			}else{
				notvalidVotes++	
			}
		}
	}

	// Check the votes
	if(positiveVotes > negativeVotes){
		return true
	}else{
		return false
	}
}

func NewHeartbeatMonitor() *HeartbeatMonitor {
	hbm := &HeartbeatMonitor{}
	hbm.timer = time.AfterFunc(electionTimeout*time.Second, func() {
		// For avoiding deadlocks, wait a random amount of time (150-1999 milliseconds)
		time.Sleep(time.Duration(150+rand.Intn(1850)) * time.Millisecond)

		// Quick check
		if(thisNode.State != "LEADER"){
			// Register operation in personal log
			appendOperation("ELECTION-START" , time.Now().Format("15:04:05.000") , "Started by: " + thisNode.Name)
			
			// Memorize the starting time
			convergeTime = time.Now()		

			// Try to be the new leader
			sendRequestVote()
		}
		
	})

	return hbm
}

// Implement the homonym service inside sdcc.proto
func  (sl *serverL) ShareElectionResult(ctx context.Context, msg *pb.LeaderContacts) (*emptypb.Empty, error){
	// The receiver node registers the new node as leader
	setTheLeader(msg.Leadername ,msg.Leaderport)
	appendOperation("NEW-LEADER-IS" , time.Now().Format("15:04:05.000") , "ServerName: " + msg.Leadername + "ServerPort: " + msg.Leaderport)

	return &emptypb.Empty{}, nil
}

// Send leader name and port to a target node
func sendLeaderContacts(leaderName, leaderPort, target string){
	//conn, err := grpc.Dial(target,grpc.WithInsecure(),grpc.WithBlock(),)
	conn, err := grpc.Dial(target,grpc.WithInsecure(),)

	if err != nil {
		log.Println(err)
		//return "error"
	}

	defer conn.Close()

	client := pb.NewLeadershipClient(conn)

	
	client.ShareElectionResult(context.Background(),&pb.LeaderContacts{From: thisNode.Name,Leadername: leaderName,Leaderport: leaderPort,},)
	

	/*
	reply, err := client.ShareElectionResult(context.Background(),&pb.LeaderContacts{From: from,Leadername: leaderName,Leaderport: leaderPort,},)
	if err != nil {
		log.Println(err)
		return "error"
	}
	return ""
	*/

}

// Send a RequestVote rpc to every other peer
// Can't do it if already voted for a node
func sendRequestVote(){
	// The node can try to vote for itself and asks for votes
	if(!thisNode.AlreadyVoted){
		thisNode.State = "CANDIDATE"
		receivedVotes := []string{}
		thisNode.AlreadyVoted = true	//voted for itself
		for _, peerInfo := range thisNode.Peers {
			receivedVotes = append(receivedVotes, requestVote(thisNode.Name, peerInfo, thisNode.RaftTerm) )
		}

		// Check if the node received the majority of the votes
		results := strings.Join(receivedVotes," ")
		customPrintln("VOTES: " + results)

		// Check if the node is a candidate - CONTROLLO ECCESSIVAMENTE ZELANTE
		//if(thisNode.State == "CANDIDATE"){



			if(majorityChecker(receivedVotes)){
				// Assume leader role
				customPrintln("Majority obtained")
				thisNode.State = "LEADER"
				thisNode.AlreadyVoted = false
				thisNode.LeaderName = thisNode.Name
				thisNode.LeaderPort = thisNode.Port

				// Register operation in personal log
				appendOperation("ELECTED-AS-LEADER" , time.Now().Format("15:04:05.000") , "ServerName: " + thisNode.Name)

				// Register needed time for leader election in personal log
				appendOperation("ELECTION-TIME", time.Since(convergeTime).String() , "Time needed for the election of " + thisNode.Name)

				//setTheLeader(thisNode.Name, thisNode.Port)
				//go sendPeriodicHeartbeats()

				// Notify other nodes of the new leader
				for _, peerInfo := range thisNode.Peers {
					customPrintln("Telling to: " + peerInfo)
					sendLeaderContacts(thisNode.Name, thisNode.Port, peerInfo)
					sendHeartbeat(thisNode.Name, peerInfo, "heartbeat")
				}
			// The node stays a follower without the majority
			}else{
				customPrintln("Majority NOT obtained")
				thisNode.State = "FOLLOWER"
			}

		/*
		// The node is a follower - can't procede with the election
		}else{
			//thisNode.AlreadyVoted = false
			customPrintln("The node was demoted to a follower - there must be already a leader")
		}
		*/

	// This node already voted for another node
	}else{
		customPrintln("Already voted this round - This node can't try to be leader")
	}

}
//------------------------------------------------------------------------------------------------------------------------------


// Call this whenever the node receives a heartbeat.
func (m *HeartbeatMonitor) HeartbeatReceived() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.timer.Stop()
	m.timer.Reset(electionTimeout * time.Second)
}

// Implement the homonym service inside sdcc.proto
func (sl *serverL) Heartbeat(ctx context.Context, msg *pb.Message) (*emptypb.Empty, error){
	customPrintln("Received heartbeat from: " + msg.From)

	// Check the validity of the heartbeat
	if(msg.From == thisNode.LeaderName){
		thisNode.HBMonitor.HeartbeatReceived()

		thisNode.State = "FOLLOWER"
		thisNode.AlreadyVoted = false
		return &emptypb.Empty{}, nil
	// Received invalid heartbeat
	}else{
		customPrintln("The node: " + msg.From + " it's not the leader anymore")
		return &emptypb.Empty{}, nil
	}
}

// Using Service "Leadership"
// Send an heartbeat to a target node
func sendHeartbeat(from, target, messageText string) string{
	customPrintln("Sending heartbeat to " + target)
	conn, err := grpc.Dial(target,grpc.WithInsecure(),)

	if err != nil {
		log.Println(err)
		return "error"
	}
	defer conn.Close()

	client := pb.NewLeadershipClient(conn)

	reply, err := client.Heartbeat(context.Background(),&pb.Message{From: from,Text: messageText,},)
	if err != nil {
		log.Println(err)
		return "error"
	}
	fmt.Println("[DEBUG] received empty reply:", reply)
	return ""
}


// The leader sends heartbeats periodically to the other Consensus nodes
func sendPeriodicHeartbeats(){
	for {
		if(thisNode.State == "LEADER"){
			for _, peerInfo := range thisNode.Peers {
				sendHeartbeat(thisNode.Name, peerInfo, "heartbeat")
			}
			time.Sleep(heartbeatPeriod * time.Second)
		}
	}
}

//------------------------------------------------------------------------------------------------------------------------------
func main() {
	// Print a short welcoming message at the start
	greetings()

	// Initial node setup
	setUpNodeInfo()
	thisNode.HBMonitor = NewHeartbeatMonitor()
	
	// DEBUG - set node 3 as the leader
	//setTheLeader("cNode03","50053")
	
	// Every node starts to listen for messages - use goroutine
	go startServer(thisNode.Name, thisNode.Port)

	// Every node runs this
	// Only the leader sends heartbeats
	go sendPeriodicHeartbeats()

	// Force the container to stay active - unlike an empty for this is more resource-friendly
	select {}
}