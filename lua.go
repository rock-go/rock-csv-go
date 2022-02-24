package csv

import (
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/xbase"
)

var xEnv *xbase.EnvT

func cvsL(L *lua.LState) int {
	n := L.GetTop()
	if n == 0 {
		L.RaiseError("invalid csv load option , must be csv.load(filename , [seek])")
		return 0
	}

	filename := L.CheckString(1)
	var seek int64
	if n == 2 {
		seek = L.CheckInt64(64)
	}

	ud := L.NewAnyData(newCsvGo(filename, seek))
	L.Push(ud)
	return 1
}

func LuaInjectApi(env *xbase.EnvT) {
	xEnv = env
	env.Set("csv", lua.NewFunction(cvsL))
}
