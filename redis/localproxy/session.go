package main

import (
	"bufio"
	"errors"
	"github.com/go-redis/redis"
	"github.com/go-redis/redis/internal/proto"
	"io"
	"log"
	"net"
)

type RedisProto byte

const (
	TypeString    RedisProto = '+'
	TypeError     RedisProto = '-'
	TypeInt       RedisProto = ':'
	TypeBulkBytes RedisProto = '$'
	TypeArray     RedisProto = '*'
)

type Session struct {
	con  net.Conn
	rc   *redis.ClusterClient
	conf *Config
	quit bool
	r    *proto.Reader
	w    *bufio.Writer
	//rdbuf []byte
}

var errProtoError = errors.New("Redis Proto Error")
var errArgvError = errors.New("Argv Error")

type QeqCommandinfo struct {
	buf []byte
	//argvs []interface{}
	argvs []string
}

func (q *QeqCommandinfo) Tostring() (s string) {
	for _, argv := range q.argvs {
		s += argv
	}
	return s
}

func NewSession(con net.Conn,
	rc *redis.ClusterClient, conf *Config) (*Session, error) {
	s := &Session{con: con, rc: rc, conf: conf}
	s.r = proto.NewReader(con)
	s.w = bufio.NewWriter(con)
	return s, nil
}

func (s *Session) Start(cq chan struct{}) {
	ce := make(chan error, 1)
	defer func() {
		s.close()
		close(ce)
	}()
	go func() {
		select {
		case <-ce:
			return
		case <-cq:
			s.quit = true
			log.Printf("[%p] Session Quit by proxy\n", s)
		}
	}()

	for !s.quit {
		var (
			err error
		)
		reqs := make([]*QeqCommandinfo, 0, 1)
		var nostart = true
		for nostart || s.r.HasMore() {
			req := &QeqCommandinfo{argvs: make([]string, 0, 1),
				buf: make([]byte, 0, 1024)}
			//req.buf = [] byte
			nostart = false
			if req.argvs, err = s.r.ReadReq(); err != nil {
				if err == io.EOF { //client close
					//log.Printf("[%p] Session Stop\n", s)
					return
				}
				log.Printf("[%p] xxxxxxxxxxSession other error %s\n", s, err.Error())
				continue
			}

			reqs = append(reqs, req)
		}
		if len(reqs) > 0 {
			if err = s.ProcCmds(reqs); err != nil {
				log.Printf("[%p] Session ProcCmds err [%q]\n", s, err)
				continue
			}
		}
	}
}

func (s *Session) ProcCmds(reqCmds []*QeqCommandinfo) error {
	lencmds := len(reqCmds)
	s.w.Reset(s.con)
	var sendbuf []byte
	if 1 == lencmds { //one cmd
		tmpSlice := make([]interface{}, len(reqCmds[0].argvs))
		for i, v := range reqCmds[0].argvs {
			tmpSlice[i] = v
		}
		cmd := redis.NewCmd(tmpSlice...)
		s.rc.Process(cmd)
		if cmd.Err() != nil {
			sendbuf = ([]byte)(cmd.Err().Error())
		} else {
			sendbuf = cmd.GetResbuf()
		}
		if _, err := s.w.Write(sendbuf); err != nil {
			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n", s, cmd.String(), cmd.Err())
			return err
		}
		cmd.SetResbuf(nil)
	} else if lencmds > 1 { //pipline
		var (
			cmdres = make([]redis.Cmder, len(reqCmds))
			err    error
			pip    = s.rc.Pipeline()
		)
		for _, cmdinfo := range reqCmds {
			tmpSlice := make([]interface{}, len(cmdinfo.argvs))
			for i, v := range cmdinfo.argvs {
				tmpSlice[i] = v
			}
			cmd := redis.NewCmd(tmpSlice...)
			pip.Process(cmd)
		}
		if cmdres, err = pip.Exec(); err != nil {
			pip.Close()
			log.Printf("[%p] Session pipe opt  err [%v]\n", s, err.Error())
			return err
		}
		var res = []byte{}
		for _, cmd := range cmdres {
			res = append(res, cmd.GetResbuf()...)
		}
		if _, err := s.con.Write(res); err != nil {
			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n",
				s, cmdres[0].String(), err.Error())
			pip.Close()
			return err
		}
		pip.Close()
	}
	s.w.Flush()
	return nil
}

func (s *Session) close() error {
	if s.con != nil {
		s.r.Clear()
		s.w.Reset(nil)
		if t, ok := s.con.(*net.TCPConn); ok {
			return t.Close()
		}
		return s.con.Close()
	}
	return nil
}
