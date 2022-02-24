package csv

import (
	"github.com/rock-go/rock/lua"
	"strings"
)

type row []string

func (r row) String() string                         { return strings.Join(r, ",") }
func (r row) Type() lua.LValueType                   { return lua.LTObject }
func (r row) AssertFloat64() (float64, bool)         { return 0, false }
func (r row) AssertString() (string, bool)           { return "", false }
func (r row) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (r row) Peek() lua.LValue                       { return r }

func (r row) Len() int {
	return len(r)
}

func (r row) GetField(L *lua.LState, key lua.LValue) lua.LValue {
	idx := lua.CheckInt(L, key)

	if idx >= r.Len() {
		return lua.LNil
	}
	return lua.S2L(r[idx])
}
