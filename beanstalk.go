package gobsc

import (
	"errors"
	"strconv"
	"time"

	"github.com/kr/beanstalk"
)

var (
	errInvalidScheme = errors.New("invalid beanstalk server")
)

type BeanstalkConn struct {
	beanstalk.Conn
}

func (b *BeanstalkConn) Close() {
	_ = b.Conn.Close()
}

func newBeanstalkFactory(ip string, port int) Factory {
	return func() (Resource, error) {
		return connectBeanstalkd(ip, strconv.Itoa(port))
	}
}

func newBeanstalkPool(ip string, port int, capacity int, maxCapacity int, idleTimout time.Duration) *ResourcePool {
	return NewResourcePool(newBeanstalkFactory(ip, port), capacity, maxCapacity, idleTimout)
}

func connectBeanstalkd(ip string, port string) (*BeanstalkConn, error) {
	addr := ip + ":" + port
	c, err := beanstalk.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &BeanstalkConn{Conn: *c}, nil
}
