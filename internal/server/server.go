package server

import (
	"context"
	log_v1 "playground/dsingo/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ log_v1.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	log_v1.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcServer) Produce(ctx context.Context, req *log_v1.ProduceRequest) (*log_v1.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &log_v1.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *log_v1.ConsumeRequest) (*log_v1.ConsumeResponse, error) {
	rec, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &log_v1.ConsumeResponse{Record: rec}, nil
}

func (s *grpcServer) ProduceStream(stream log_v1.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *log_v1.ConsumeRequest, stream log_v1.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

type CommitLog interface {
	Append(*log_v1.Record) (uint64, error)
	Read(uint64) (*log_v1.Record, error)
}
