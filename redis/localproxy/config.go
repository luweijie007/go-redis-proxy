package main

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	Net            string   `json:"Net"`
	ProxyAddr      string   `json:"ProxyAddr"`
	NodeConNum     int      `json:"NodeConNum"`
	ReadTimeout    int      `json:"ReadTimeout"`
	WriteTimeout   int      `json:"WriteTimeout"`
	RegetStateSpan int      `json:"RegetStateSpan"`
	RedisAddrs     []string `json:"RedisAddrs"`
}

func (self *Config) Load(filename string) error {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	datajson := []byte(data)
	err = json.Unmarshal(datajson, self)
	if err != nil {
		return err
	}
	return nil
}
