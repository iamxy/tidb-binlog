### Drainer支持插件的用法
#### 1.1 为什么支持插件
目前Drainer已经支持file, MySQL, TiDB, Kafka等多种通用下游组件。但是在实际使用中，一些用户所在的公司对通用组件进行了定制化或者使用了公司内部的组件，此时Drainer便无法满足要求。

针对上述问题，Drainer提供了插件的形式，供用户针对特定的下游组件进行定制化开发，从而满足公司内部业务场景的需求。
#### 1.2 插件原理
Drainer会调用`Syncer`接口，将解析出的Binlog同步到各个下游组件中。用户只需要定制化实现`Syncer`接口即可。

定制化`Syncer`时候，需要实现`Syncer`的如下接口：

```
// Syncer sync binlog item to downstream
type Syncer interface {
	// Sync the binlog item to downstream
	Sync(item *Item) error
	// will be close if Close normally or meet error, call Error() to check it
	Successes() <-chan *Item
	// Return not nil if fail to sync data to downstream or nil if closed normally
	Error() <-chan error
	// Close the Syncer, no more item can be added by `Sync`
	// will drain all items and return nil if all successfully sync into downstream
	Close() error
	// SetSafeMode make the Syncer to use safe mode or not. If no need to handle, it should return false
	SetSafeMode(mode bool) bool
}
```

用户需要将上述接口进行实现，并编译成动态库(*.so)形式，通过Drainer的启动参数进行加载。

我们已经实现了插件框架，用户在定制化插件时只需要关注各个`Syncer`接口的实现即可。

#### 1.3 Demo介绍
为了向用户展示Drainer插件的用法，我们在源码中编写了一个Demo。Demo涉及的文件如下：

```
# 需要实现 Syncer 接口函数，主要是业务逻辑相关的内容
./drainer/sync/plugin_demo.go 
# 胶水代码，用来将 业务逻辑代码与插件代码进行耦合，基本不需要用户修改
./drainer/syncplg/syncerdemo/syncerdemo.go
# 用来将 业务代码 编译成 插件（动态库），不需要用户修改
./drainer/syncplg/syncerdemo/Makefile
```

编写一个插件的**步骤如下**：

- 步骤一：plugin_demo.go文件实现各个接口

该文件中主要是需要用户实现的 `Syncer`接口的各个函数，例如例子中，我们只对binlog进行简单打印，核心代码集中在 `Sync(item *Item)`函数中，如下所示：

```
func (sd *SyncerDemo) Sync(item *Item) error {
	//demo
	log.Info("item", zap.String("%s", fmt.Sprintf("%v", item)))
	sd.success <- item
	return nil
}
```
- 步骤二：编译

```
cd ./drainer/syncplg/syncerdemo/
make
```

如果没有报错，会生成插件，如下：

```
syncerdemo.so
```

- 步骤三：配置启动参数

```
# 指定Syncer使用插件
dest-db-type = "plugin"
# 需要加载的插件的名称
plugin-name = "syncerdemo.so"
# 插件所在路径
plugin-path = "./drainer/syncplg/syncerdemo/"
# 插件内部可以用的配置文件，如果使用不到，可以不配置
plugin-cfg-path = "/drainer/syncplg/syncerdemo/plgcfg.toml"
```

加载插件后，可以通过Drainer的运行日志（`./conf/drainer.log`）来查看参数是否已经正常加载；也可以用来查看插件是否加载成功。