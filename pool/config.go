package pool

import (
	"errors"
	"time"
)

type Config struct {
	Host        string        `yaml:"host"`
	Port        int           `yaml:"port"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	Database    string        `yaml:"database"`
	MaxConn     int32         `yaml:"maxConn"`
	MinConn     int32         `yaml:"minConn"`
	MaxIdleTime time.Duration `yaml:"maxIdleTime"`
	TimeZone    string        `yaml:"timezone"`
	DisableTLS  bool          `yaml:"disableTLS"`
}

func (c *Config) Validate() error {
	if c.Host == `` {
		return errors.New(`empty host`)
	}
	if c.Port == 0 {
		return errors.New(`port is zero`)
	}
	if c.Username == `` {
		return errors.New(`empty username`)
	}
	if c.Password == `` {
		return errors.New(`empty password`)
	}
	if c.Database == `` {
		return errors.New(`empty database`)
	}
	if c.MaxConn == 0 {
		return errors.New(`maxCons is zero`)
	}
	if c.MaxIdleTime < time.Second {
		return errors.New(`maxIdleTime is less than 1 second`)
	}
	if c.TimeZone == `` {
		return errors.New(`empty timezone`)
	}
	return nil
}
