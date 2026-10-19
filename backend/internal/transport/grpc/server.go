package grpc

import (
	roomagent "IM_backend/internal/transport/grpc/propto"
	"net"

	googlegrpc "google.golang.org/grpc"
)

type Server struct {
	listener net.Listener
	server   *googlegrpc.Server
}

func NewGrpcServer(
	address string,
	service roomagent.RoomAgentContextServiceServer,
	options ...googlegrpc.ServerOption,
) (*Server, error) {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return nil, err
	}

	server := googlegrpc.NewServer(options...)
	roomagent.RegisterRoomAgentContextServiceServer(server, service)

	return &Server{
		listener: listener,
		server:   server,
	}, nil
}

func (s *Server) Serve() error {
	return s.server.Serve(s.listener)
}

func (s *Server) GracefulStop() {
	s.server.GracefulStop()
}

func (s *Server) Stop() {
	s.server.Stop()
}
