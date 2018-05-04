package main

import (
	"github.com/docopt/docopt-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	//log setting
	log.SetFlags(log.Llongfile | log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	const usage = `
	Usage: localproxy [--cf = "localproxy.json"]
		   localproxy   --version
	Options:
			--cf=cf file defalut localproxy.json
			--version version info.
	`
	version := "0.0.0"
	d, err := docopt.Parse(usage, nil, false, version, false, false)
	if err != nil {
		log.Fatal(err, "parse arguments failed\n")
	}
	var cfgFile string

	if v, ok := d["--cf"]; ok {
		cfgFile = v.(string)
	} else {
		log.Fatal(err, "cfg NO found cf\n")
	}
	var conf Config
	if err = conf.Load(cfgFile); err != nil {
		log.Panic(err, "log cf failed err [%s]\n", err.Error())
	}
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
