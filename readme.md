# csv
导入csv数据结构

## rock.csv
- anydata = rock.csv(file)
- file 文件路径

#### 内部方法
- [anydata.pipe(v)]()
```lua
    local any = rock.csv("a.csv")
    any.pipe(function(row)
        print(row[1])
        print(row[2])
    end)
```