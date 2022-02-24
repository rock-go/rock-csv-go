package csv

import (
	"encoding/csv"
	"github.com/rock-go/rock/logger"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/pipe"
	"io"
	"os"
)

type csvGo struct {
	done chan struct{}

	filename string
	fd       *os.File
	reader   *csv.Reader
	err      error
	eof      bool
	seek     int64
}

func newCsvGo(filename string, seek int64) *csvGo {
	return &csvGo{
		done:     make(chan struct{}, 1),
		filename: filename,
		seek:     seek,
	}
}

func (c *csvGo) Fd() *os.File {
	if c.fd != nil {
		return c.fd
	}

	fd, err := os.Open(c.filename)
	if err != nil {
		c.err = err
		return fd
	}

	if c.seek != 0 {
		if _, e := fd.Seek(c.seek, io.SeekStart); e != nil {
			c.err = e
			return nil
		}
	}

	c.fd = fd
	return fd
}

func (c *csvGo) Reader() *csv.Reader {
	if c.reader != nil {
		return c.reader
	}

	fd := c.Fd()
	if fd == nil {
		return nil
	}

	reader := csv.NewReader(fd)
	c.reader = reader

	return reader
}

func (c *csvGo) Next(L *lua.LState) int {
	r := c.Reader()
	if r == nil {
		L.Push(lua.LNil)
		L.Push(lua.S2L(c.err.Error()))
		return 2
	}

	line, e := r.Read()
	if e != nil {
		c.err = e
		L.Push(lua.LNil)
		L.Push(lua.S2L(e.Error()))
		return 2
	}
	L.Push(row(line))
	return 1
}

func (c *csvGo) pipeL(L *lua.LState) int {
	r := c.Reader()
	if r == nil {
		L.Push(lua.S2L(c.err.Error()))
		return 1
	}

	pv := pipe.LFunc(L.CheckFunction(1))
	for {
		select {
		case <-c.done:
			return 0

		default:
			line, err := r.Read()
			if err != nil {
				if err == io.EOF {
					return 0
				}
				L.Push(lua.S2L(err.Error()))
				return 1
			}

			if e := pv(row(line) , L) ; e != nil {
				L.Push(e)
				return 1
			}
		}
	}

}

func (c *csvGo) Close(L *lua.LState) int {
	c.done <- struct{}{}

	if c.fd == nil {
		return 0
	}

	if e := c.fd.Close(); e != nil {
		logger.Errorf("cvs %s file close error %v", c.filename, e)
		return 0
	}

	logger.Errorf("cvs %s file close succeed", c.filename)
	return 0
}

func (c *csvGo) Get(L *lua.LState, key string) lua.LValue {
	switch key {
	case "pipe":
		return L.NewFunction(c.pipeL)

	case "next":
		return L.NewFunction(c.Next)

	case "close":
		return L.NewFunction(c.Close)

	case "err":
		return lua.S2L(c.err.Error())

	case "eof":
		return lua.LBool(c.err == io.EOF)

	default:
		return lua.LNil
	}
}
