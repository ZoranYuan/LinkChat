package grpc

import (
	"net"

	googlegrpc "google.golang.org/grpc"

	groupagent "IM_backend/core/interfaces/grpc/pb"
)

type Server struct {
	listener net.Listener
	server   *googlegrpc.Server
}

func NewServer(
	address string,
	service groupagent.GroupAgentContextServiceServer,
	options ...googlegrpc.ServerOption,
) (*Server, error) {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return nil, err
	}

	server := googlegrpc.NewServer(options...)
	groupagent.RegisterGroupAgentContextServiceServer(server, service)

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
