package gobsc

import (
	"sync"

	"time"

	"golang.org/x/net/context"
)

const (
	DefaultBeanstalkdServerIP   = "127.0.0.1"
	DefaultBeanstalkdServerPort = 11300
	DefaultConnectionsNum       = 10
)

var (
	pool        *ResourcePool
	ctx         context.Context
	initMutex   sync.Mutex
	initialized bool
)

type BeanstalkSetting struct {
	BeanstalkdServerIP   string
	BeanstalkdServerPort int
	ConnectionsNum       int
}

var beanstalkdSetting BeanstalkSetting

func Init() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if !initialized {
		ctx = context.Background()
		// new pool
		pool = newBeanstalkPool(beanstalkdSetting.BeanstalkdServerIP, beanstalkdSetting.BeanstalkdServerPort, beanstalkdSetting.ConnectionsNum, beanstalkdSetting.ConnectionsNum, time.Minute)
		initialized = true
	}
}

func GetSettings() BeanstalkSetting {
	if len(beanstalkdSetting.BeanstalkdServerIP) == 0 {
		beanstalkdSetting.BeanstalkdServerIP = DefaultBeanstalkdServerIP
	}
	if beanstalkdSetting.BeanstalkdServerPort == 0 {
		beanstalkdSetting.BeanstalkdServerPort = DefaultBeanstalkdServerPort
	}
	if beanstalkdSetting.ConnectionsNum == 0 {
		beanstalkdSetting.ConnectionsNum = DefaultConnectionsNum
	}
	return beanstalkdSetting
}

func SetSettings(beanstalkdServerIP string, beanstalkdServerPort int, connectionsNum int) {
	beanstalkdSetting.BeanstalkdServerIP = beanstalkdServerIP
	beanstalkdSetting.BeanstalkdServerPort = beanstalkdServerPort
	beanstalkdSetting.ConnectionsNum = connectionsNum
}

func GetConnection() (*BeanstalkConn, error) {
	if !initialized {
		Init()
	}
	resource, err := pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	return resource.(*BeanstalkConn), nil
}

func PutConnection(conn *BeanstalkConn) {
	pool.Put(conn)
}

func Close() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if initialized {
		pool.Close()
		initialized = false
	}

}
