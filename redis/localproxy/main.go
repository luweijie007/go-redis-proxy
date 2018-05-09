package main

import (
	"github.com/docopt/docopt-go"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"strconv"
)

func main() {
	//log setting
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	const usage = `
	Usage: localproxy config [<filepath>]
			localproxy arg [--Net=<net> --Listenaddr=<addr> --NodeConNum=<num> --RegetStateSpan=<second>] (--RedisAddrs=addr)
			localproxy   --version
	Options:
			filepath defalut localproxy.json
			Net defalut: tcp4
			listenaddr default 127.0.0.1:19000
			RedisAddrs redis cluster addrs split by comma
			NodeConNum default 8
			ReGetstateSpan reget state time span default 5 second 
			--version version info
	`
	version := "0.0.0"
	d, err := docopt.ParseArgs(usage, nil, version)
	//d, err := docopt.Parse(usage, nil, false, version, false, false)
	if err != nil {
		log.Fatal(err, "parse arguments failed\n")
	}
	var conf = Config{Net: "tcp4", ProxyAddr: "127.0.0.1:19000", NodeConNum: 8,
						RegetStateSpan:5}
	if bConf, _ := d.Bool("config"); bConf {
		if confpath, e := d.String("<filepath>"); e == nil {
			if err = conf.Load(confpath); err != nil {
				log.Panic(err, "log cf failed err [%s]\n", err.Error())
			}
		}
	} else if barg, _ := d.Bool("arg"); barg {
		if s, e := d.String("--Net"); e == nil {
			conf.Net = s
		}
		if s, e := d.String("--Listenaddr"); e == nil {
			conf.ProxyAddr = s
		}
		if s, e := d.String("--RedisAddrs"); e == nil {
			conf.RedisAddrs = strings.Split(s, ",")
		}
		if s, e := d.String("--ReGetstateSpan"); e == nil {
			conf.RegetStateSpan,_ = strconv.Atoi(s)
		}
	} else {
		log.Fatal(err, "Input argv err \n")
	}
	//fmt.Printf("conf %+v\n", conf)
	var proxy = &Proxy{}
	if proxy, err = NewPorxy(&conf); err != nil {
		log.Fatalf("create proxy fail err [%s]\n", err.Error())
	}
	if err := proxy.Start(); err != nil {
		log.Fatalf("start proxy fail err [%s]\n", err.Error())
	}

	//singal quit
	go func() {
		defer proxy.Close()
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
		sig := <-c
		log.Printf("[%p] proxy receive signal = '%v'", proxy, sig)
		time.Sleep(4)
	}()

	//wait to quit
	<-proxy.exit.C
	log.Printf("[%p] proxy quit", proxy)
}
