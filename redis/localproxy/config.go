package main

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ProxyAddr    string   `json:"proxyAddr"`
	NodeConNum   int      `json:"NodeConNum"`
	ReadTimeout  int      `json:"ReadTimeout"`
	WriteTimeout int      `json:"WriteTimeout"`
	RedisAddrs   []string `json:"RedisAddrs"`
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
