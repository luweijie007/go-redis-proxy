package main

import (
	"errors"
	"log"
	"net"
	"time"

	"github.com/go-redis/redis"
)

type Proxy struct {
	exit struct {
		C chan struct{}
	}
	cf *Config
	l  net.Listener
	rc *redis.ClusterClient
}

var errFail = errors.New("Porxy Run Fail")

func (p *Proxy) Close() {
	//close(p.exit.C)
	if p.l != nil {
		p.l.Close()
	}
	if p.rc != nil {
		p.rc.Close()
	}
}

func (p *Proxy) Setup() error {
	var err error
	if p.l, err = net.Listen(p.cf.Net, p.cf.ProxyAddr); err != nil {
		log.Fatalf("Listen err addr [%s] err [%s]\n",
			p.cf.ProxyAddr, err.Error())
		return err

	}
	if err = p.initRedisCluster(); err != nil {
		log.Fatalf("initRedisCluster err addr [%v] err [%s]\n",
			p.cf.RedisAddrs, err.Error())
		return err
	}
	return nil
}

func (p *Proxy) acceptConn() (net.Conn, error) {
	for {
		conn, err := p.l.Accept()
		if e, ok := err.(net.Error); ok &&
			(e.Temporary() || e.Timeout()) {
			log.Printf("[%p] proxy accept new connection failed\n", p)
			time.Sleep(time.Second * 1)
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetNoDelay(true)
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(time.Second * 2)
		}
		conn.SetDeadline(time.Time{})
		//log.Printf("Proxy [%p] acept client Addr [%s]\n", p, conn.RemoteAddr().String())
		return conn, err
	}

}

func (p *Proxy) initRedisCluster() error {
	p.rc = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    p.cf.RedisAddrs,
		PoolSize: p.cf.NodeConNum,
	})
	if rs := p.rc.Ping(); rs.Err() != nil {
		log.Printf("proxy initRedisCluster failed\n")
		return rs.Err()
	}
	return nil
}

func (p *Proxy) Start() error {
	defer func() {
		log.Printf("proxy exit\n")
		p.Close()
		time.Sleep(time.Second * 10)
	}()
	ech := make(chan error, 1)
	log.Print("proxy start\n")
	go func() (err error) {
		defer func() {
			ech <- err
		}()
		for {
			conn, err := p.acceptConn()
			if err != nil {
				return err
			}
			if s, err := NewSession(conn, p.rc, p.cf); err != nil {
				log.Printf("proxy NewSession failed %s\n", err.Error())
			} else {
				go s.Start(p.exit.C)
			}
		}
	}()
	//proxy exit
	select {
	case <-p.exit.C:
		log.Printf("proxy [%p] shutdown", p)
	case err := <-ech:
		log.Printf("proxy [%p] exit on error %v", p, err)
	}
	log.Printf("proxy [%p] quit\n", p)
	return nil
}

func NewPorxy(config *Config) (*Proxy, error) {
	proxy := &Proxy{cf: config,
		exit: struct{ C chan struct{} }{C: make(chan struct{}, 1)}}
	if err := proxy.Setup(); err != nil {
		log.Fatalf("NewPorxy Setup fail err [%s]\n", err.Error())
		return nil, errFail
	}
	return proxy, nil
}
