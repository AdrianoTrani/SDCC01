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
	pb.RegisterReadingWritingServer(g, &serverRW{name: myname})

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
	// Leader receives it from the Client Proxy
	if(thisNode.State == "LEADER"){
		// Print the received message
		customPrintln("Leader received from "+ msg.From + " the Message: " + msg.Text)

		// Register operation in personal log
		appendOperation("READKEY" , time.Now().Format("15:04:05.000") , "Key: " + msg.Text)

		// Send the operation to the followers
		for _, peerInfo := range thisNode.CNodes {
			customPrintln("Telling to: " + peerInfo)
			conn, err := grpc.Dial(peerInfo,grpc.WithInsecure(),)

			// In case of errors with Dial, try with another node
			if err != nil {
				continue
			}

			// Pospone the closing
			defer conn.Close()

			// Create the client for the service "ReadingWriting"
			client := pb.NewReadingWritingClient(conn)

			// Send the message and obtain the Reply
			reply, err := client.SearchKey(context.Background(),&pb.Message{From: thisNode.Name+":"+thisNode.Port,Text: msg.Text,},)
			if err != nil {
				continue
			}
			customPrintln("Leader received from "+ peerInfo + " the Message: " + reply.Status)
		}
	// Follower receives it from the Leader Node
	}else{
		customPrintln("Follower received from "+ msg.From + " the Message: " + msg.Text)

		// Register operation in personal log
		appendOperation("READKEY" , time.Now().Format("15:04:05.000") , "Key: " + msg.Text)
	}
	
	// Build the reply to the message
	//	Leader reads its own volume and answers to the Client Proxy
	//	Follower reads its own volume and answers to the Leader Client
	return &pb.Reply{Status: getValue(msg.Text),}, nil
}

// Implement the homonym service inside sdcc.proto
func (srw *serverRW) AddPair(ctx context.Context, pair *pb.PairKeyValue) (*pb.Reply, error) {
	// Leader receives it from the Client Proxy
	if(thisNode.State == "LEADER"){
		// Print the received pair
		customPrintln("Leader received: (" + pair.Key + "," + pair.Value + ")")
	
		// Register operation in personal log
		appendOperation("ADDPAIR" , time.Now().Format("15:04:05.000") , "(" + pair.Key + "," + pair.Value + ")")

		// Send the operation to the followers
		for _, peerInfo := range thisNode.CNodes {
			customPrintln("Telling to: " + peerInfo)
			conn, err := grpc.Dial(peerInfo,grpc.WithInsecure(),)

			// In case of errors with Dial, try with another node
			if err != nil {
				continue
			}

			// Pospone the closing
			defer conn.Close()

			// Create the client for the service "ReadingWriting"
			client := pb.NewReadingWritingClient(conn)

			// Send the message and obtain the Reply
			reply, err := client.AddPair(context.Background(),&pb.PairKeyValue{Key: pair.Key,Value: pair.Value,},)
			if err != nil {
				continue
			}
			customPrintln("Leader received from "+ peerInfo + " the Message: " + reply.Status)
		}



	// Follower receives it from the Leader Node
	}else{
		customPrintln("Follower received from leader: (" + pair.Key + "," + pair.Value + ")")

		// Register operation in personal log
		appendOperation("ADDPAIR" , time.Now().Format("15:04:05.000") , "(" + pair.Key + "," + pair.Value + ")")

	}

	// Build the reply to the message
	//	Leader writes on its own volume and answers to the Client Proxy
	//	Follower writes on its own volume and answers to the Leader Client
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
	customPrintln("This is the Leader address that I know: " + thisNode.LeaderName + ","+ thisNode.LeaderPort)
	return &pb.LeaderContacts{Leadername: thisNode.LeaderName,Leaderport: thisNode.LeaderPort,}, nil
}

//------------------------------------------------------------------------------------------------------------------------------
