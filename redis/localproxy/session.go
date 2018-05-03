package main

import (
	"errors"
	"github.com/go-redis/redis"
	"github.com/go-redis/redis/internal/proto"
	"log"
	"net"
	"bufio"
)

type RedisProto byte

const (
	TypeString    RedisProto = '+'
	TypeError     RedisProto = '-'
	TypeInt       RedisProto = ':'
	TypeBulkBytes RedisProto = '$'
	TypeArray     RedisProto = '*'
	//'\r\n'
	TypeRe0 RedisProto = '\r'
	TypeRe1 RedisProto = '\n'
)

type Session struct {
	con  net.Conn
	rc   *redis.ClusterClient
	conf *Config
	quit bool
	r  *proto.Reader
	w *bufio.Writer
	//rdbuf []byte
}

var errProtoError = errors.New("Redis Proto Error")
var errArgvError = errors.New("Argv Error")

type QeqCommandinfo struct {
	buf   []byte
	argvs []interface{}
	//argvs []string
}

func (q *QeqCommandinfo) Tostring() (s string) {
	for _, argv := range q.argvs {
		s += argv.(string)
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

//func (s *Session) parseOnecmd(buf []byte) (*QeqCommandinfo, int, error) {
//	res := &QeqCommandinfo{}
//	//make buf no been gc
//	res.buf = buf
//	var (
//		pos    = 0
//		argn   int64
//		err    error
//		buflen = len(buf)
//	)
//	//log.Printf("parseOnecmd pos [%d] buf [%s] len(buf) %d\n", pos, buf, len(buf))
//	if RedisProto(buf[pos]) != TypeArray {
//		fmt.Println(buf)
//		log.Printf("parse err 0 [%c]\n", buf[pos])
//		return nil, 0, errProtoError
//	}
//	pos += 1
//	cur := pos
//	for cur+2 < buflen && (buf[cur] != '\r' ||
//		buf[cur+1] != '\n') {
//		cur++
//	}
//
//	if argn, err = strconv.ParseInt(string(buf[pos:cur]), 10, 32); err != nil {
//		log.Printf("parse err 1\n")
//		return nil, 0, errProtoError
//	}
//	res.argvs = make([]interface{}, 0)
//	var (
//		argCount       = 0
//		arglen   int64 = 0
//	)
//	pos = cur + 2
//	cur = pos
//	for pos < buflen {
//		if pos+1 > buflen || RedisProto(buf[pos]) != TypeBulkBytes {
//			log.Printf("parse err 2\n")
//			return nil, 0, errProtoError
//		}
//		//skipt $
//		pos++
//		cur++
//		for cur+2 < buflen && (buf[cur] != '\r' ||
//			buf[cur+1] != '\n') {
//			cur++
//		}
//		if arglen, err = strconv.ParseInt(string(buf[pos:cur]), 10, 32); err != nil {
//			return nil, 0, errProtoError
//		}
//		//skipt \r\n
//		pos = cur + 2
//		cur = pos
//		//tail with \r\n check
//		if cur+int(arglen)+2 > buflen ||
//			buf[cur+int(arglen)] != '\r' ||
//			buf[cur+int(arglen)+1] != '\n' {
//			log.Printf("parse err 3\n")
//			return nil, 0, errProtoError
//		}
//		res.argvs = append(res.argvs, string(buf[cur:cur+int(arglen)]))
//		sz := len(res.argvs)
//		t := reflect.TypeOf(res.argvs[sz-1]).String()
//		log.Printf("p %p; aaaaaaaaapend 116 argv %d %s\n", res, sz, t)
//		//log.Printf("aaaaaaaaapend 115 argv %s\n", string(res.argvs[sz-1].([]byte)))
//		argCount++
//		pos = cur + int(arglen) + 2
//		cur = pos
//		if argCount == int(argn) {
//			return res, pos, nil
//		}
//	}
//	log.Printf("parse err 4\n")
//	return nil, 0, errProtoError
//}

//func (s *Session) ParseReqcmd(buf []byte) ([]*QeqCommandinfo, error) {
//	var (
//		buftemp = buf
//		res     = make([]*QeqCommandinfo, 0)
//		cmd     *QeqCommandinfo
//		pos     int
//		err     error
//	)
//	for {
//		if cmd, pos, err = s.parseOnecmd(buftemp); err != nil {
//			log.Printf("[%p] Session parseOnecmd cmdinfo [%s] err [%v]\n", s, string(buf), err)
//			return nil, err
//		}
//		if pos >= len(buftemp)-1 {
//			res = append(res, cmd)
//			log.Println("ParseReqcmd finish cmd.argvs", len(cmd.argvs))
//			log.Println("ParseReqcmd finish cmd str ", cmd.Tostring())
//			return res, nil
//		} else {
//			log.Println("get one cmmdxxxxxx ", cmd.Tostring())
//			res = append(res, cmd)
//			buftemp = buftemp[pos:]
//		}
//	}
//}

//func (s *Session) ProcCmds(reqCmds []*QeqCommandinfo) error {
//	lencmds := len(reqCmds)
//	if 1 == lencmds { //one cmd
//		log.Println("p %; NewCmd 158xxxxxxxxxx ", reqCmds[0], reqCmds[0].Tostring())
//		cmd := redis.NewCmdBuff(reqCmds[0].argvs...)
//
//		log.Printf("NewCmd 159xxxxxxxxxx %p str %s name %s arg %d\n",
//			cmd, cmd.String(), cmd.Name(), len(cmd.Args()))
//		if err := s.rc.Process(cmd); err != nil {
//			log.Printf("[%p] Session send cmd [%s] err [%v]\n", s, cmd.String(), err)
//			return err
//		}
//		if cmd.Err() != nil {
//			log.Printf("[%p] Session opt err cmd [%s] err [%v]\n", s, cmd.String(), cmd.Err())
//			return cmd.Err()
//		}
//		var cmdRes string
//		if val, ok := cmd.Val().(string); ok {
//			cmdRes += val
//		}
//		log.Println("178  res %s \n", cmdRes)
//		if _, err := s.con.Write([]byte(cmdRes)); err != nil {
//			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n", s, cmd.String(), cmd.Err())
//			return cmd.Err()
//		}
//	} else if lencmds > 1 { //pipline
//		var (
//			cmdres = make([]redis.Cmder, 10)
//			err    error
//			pip    = s.rc.Pipeline()
//			//bufres = make([]byte, 4096)
//		)
//		defer pip.Close()
//		for _, cmdinfo := range reqCmds {
//			cmd := redis.NewCmd(cmdinfo.argvs)
//			pip.Process(cmd)
//		}
//		if cmdres, err = pip.Exec(); err != nil {
//			log.Printf("[%p] Session pipe opt  err [%v]\n", s, err.Error())
//			return err
//		}
//		var str string
//		for _, res := range cmdres {
//			str += res.(*redis.Cmd).Val().(string)
//		}
//		if _, err := s.con.Write([]byte(str)); err != nil {
//			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n",
//				s, cmdres[0].String(), err.Error())
//			return err
//		}
//	} else { //0 cmd
//		log.Printf("[%p] Session opt err cmd is zero \n", s)
//		return errArgvError
//	}
//	return nil
//}
//
//func (s *Session) ReadTotalreq() ([]byte, error) {
//	initlen := 4096
//	toalbuf := make([]byte, 0, initlen)
//	templen := 1024
//	for {
//		buf := make([]byte, templen)
//		if n, err := s.con.Read(buf); err != nil {
//			if err == io.EOF {
//				//read finish
//				log.Println("111111111")
//				toalbuf = append(toalbuf, buf[:n]...)
//				return toalbuf, nil
//			} else if err == io.ErrShortBuffer { //other error
//				log.Println("22222222")
//				toalbuf = append(toalbuf, buf[:]...)
//			} else {
//				log.Println("33333333")
//				return nil, err
//			}
//		} else if n <= len(buf) {
//			//fmt.Printf("204xxxxxxxxx [%s] %d  n ===%d\n", buf, len(buf), n)
//			toalbuf = append(toalbuf, buf[:n]...)
//			return toalbuf, nil
//		}
//	}
//}

func (s *Session) Start(cq chan struct{}) {
	defer func() {
		s.close()
	}()
	ce := make(chan error, 1)
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
			//buf  []byte
			err  error
		)
		reqs := make([] *QeqCommandinfo,1)
		for s.r.HasMore() {
			req := &QeqCommandinfo{}
			if req.argvs, err = s.r.ReadReq(); err != nil{
				log.Printf("[%p] Session ProcCmds err [%q]\n", s, err)
			}
			reqs =append(reqs, req)
		}

		if err = s.ProcCmds(reqs); err != nil {
			log.Printf("[%p] Session ProcCmds err [%q]\n", s, err)
			continue
		}
	}
}

func (s *Session) ProcCmds(reqCmds []*QeqCommandinfo) error {
	lencmds := len(reqCmds)
	if 1 == lencmds { //one cmd
		cmd := redis.NewCmd(reqCmds[0].argvs...)
		if err := s.rc.Process(cmd); err != nil {
			log.Printf("[%p] Session send cmd [%s] err [%v]\n", s, cmd.String(), err)
			return err
		}
		if cmd.Err() != nil {
			log.Printf("[%p] Session opt err cmd [%s] err [%v]\n", s, cmd.String(), cmd.Err())
			return cmd.Err()
		}
		s.w.Write(cmd.GetResbuf())
		cmd.SetResbuf(nil)
		if _, err := s.w.Write(cmd.GetResbuf()); err != nil {
			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n", s, cmd.String(), cmd.Err())
			return cmd.Err()
		}
	} else if lencmds > 1 { //pipline
		var (
			cmdres = make([]redis.Cmder, len(reqCmds))
			err    error
			pip    = s.rc.Pipeline()
			//bufres = make([]byte, 4096)
		)
		defer pip.Close()
		for _, cmdinfo := range reqCmds {
			cmd := redis.NewCmd(cmdinfo.argvs)
			pip.Process(cmd)
		}
		if cmdres, err = pip.Exec(); err != nil {
			log.Printf("[%p] Session pipe opt  err [%v]\n", s, err.Error())
			return err
		}
		var res =[] byte {}
		for _, cmd := range cmdres {
			res = append(res, cmd.GetResbuf()...)
			cmd.SetResbuf(nil)
		}
		if _, err := s.con.Write(res); err != nil {
			log.Printf("[%p] Session write to client err cmd [%s] err [%v]\n",
				s, cmdres[0].String(), err.Error())
			return err
		}
	} else { //0 cmd
		log.Printf("[%p] Session opt err cmd is zero \n", s)
		return errArgvError
	}
	return nil
}

func (s *Session) close() error {
	if s.con != nil {
		if t, ok := s.con.(*net.TCPConn); ok {
			return t.Close()
		}
		return s.con.Close()
	}
	return nil
}
