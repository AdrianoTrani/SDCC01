// Scripts related to the RPC communication used by two or more types of nodes

package main

import (
	"time"
	"context"
	"net"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "gocode/chatpb"
)

//------------------------------------------------------------------------------------------------------------------------------
type server struct {
	pb.UnimplementedChatServiceServer
	name string
}
type serverL struct {
	pb.UnimplementedLeadershipServer
	reset chan struct{}
	name string
}
type serverRW struct {
	pb.UnimplementedReadingWritingServer
	name string
}
//------------------------------------------------------------------------------------------------------------------------------
// Start server for 
//	service "ChatService"
//	service "Leadership"
//	service "ReadingWriting"
// The server will listen to the provided port
func startServer(myname, myport string) {
	lis, err := net.Listen("tcp", ":"+myport)
	if err != nil {log.Fatal(err)}

	// Create the server
	g := grpc.NewServer()

	// Register the services
	pb.RegisterChatServiceServer(g, &server{name: myname})
	pb.RegisterLeadershipServer(g, &serverL{name: myname})
	//pb.RegisterLeadershipServer(g, &serverL{name: myname,heartbeatMonitor: NewHeartbeatMonitor(),})
	pb.RegisterReadingWritingServer(g, &serverRW{name: myname})

	customPrintln(myname + " listening on " + myport)

	if err := g.Serve(lis); err != nil {log.Fatal(err)}
}
//------------------------------------------------------------------------------------------------------------------------------
// Implement the homonym service inside sdcc.proto
func (s *server) SendMessage(ctx context.Context, msg *pb.Message) (*pb.Reply, error) {

	// Print the received message
	customPrintln("Received from "+ msg.From + " the Message: " + msg.Text)

	// s.name is the name of the receiver - this part build the reply to the message
	return &pb.Reply{Status: "Hello from " + s.name,}, nil
}

// Using Service "ChatService"
// Given a sender, specify receiver and message
// Return the received Reply
func sendMessage(from, target, messageText string) string{
	customPrintln("Trying to send this Message: " + messageText + " to: " + target)

	// Create the connection
	conn, err := grpc.Dial(target,grpc.WithInsecure(),grpc.WithBlock(),)

	// In case of errors with Dial, return a message with "error" to the caller
	if err != nil {
		log.Println(err)
		return "error"
	}

	// Pospone the closing
	defer conn.Close()

	// Create the client for the service "ChatService"
	client := pb.NewChatServiceClient(conn)
	customPrintln("ChatServiceClient created")

	// Send the message and obtain the Reply
	reply, err := client.SendMessage(context.Background(),&pb.Message{From: from,Text: messageText,},)
	if err != nil {
		log.Println(err)
		return "error"
	}

	customPrintln("This is the Message that I sent: " + messageText + " from " + from)
	customPrintln("This is the Reply I received: " + reply.Status)

	// Return the Reply
	return reply.Status
}
//------------------------------------------------------------------------------------------------------------------------------
// Implement the homonym service inside sdcc.proto
func (srw *serverRW) SearchKey(ctx context.Context, msg *pb.Message) (*pb.Reply, error) {
	// Print the received message
	customPrintln("Received from "+ msg.From + " the Message: " + msg.Text)

	// Register operation in personal log
	appendOperation("READKEY" , time.Now().Format("15:04:05.000") , "Key: " + msg.Text)

	// Build the reply to the message
	return &pb.Reply{Status: getValue(msg.Text),}, nil
}

// Implement the homonym service inside sdcc.proto
func (srw *serverRW) AddPair(ctx context.Context, pair *pb.PairKeyValue) (*pb.Reply, error) {
	// Print the received pair
	customPrintln("Received: (" + pair.Key + "," + pair.Value + ")")
	
	// Register operation in personal log
	appendOperation("ADDPAIR" , time.Now().Format("15:04:05.000") , "(" + pair.Key + "," + pair.Value + ")")

	// Build the reply to the message
	return &pb.Reply{Status: writeNewPair(pair.Key, pair.Value),}, nil
}

// Implement the homonym service inside sdcc.proto
func (srw *serverRW) GetAllLog(ctx context.Context, ee *emptypb.Empty) (*pb.Reply, error){
	// Build the reply to the message
	return &pb.Reply{Status: readAllOp(),}, nil
}
//------------------------------------------------------------------------------------------------------------------------------
func (sl *serverL) LeaderInfo(ctx context.Context, ee *emptypb.Empty) (*pb.LeaderContacts, error){
	// This part build the reply to the message
	return &pb.LeaderContacts{Leadername: thisNode.LeaderName,Leaderport: thisNode.LeaderPort,}, nil
}

//------------------------------------------------------------------------------------------------------------------------------
