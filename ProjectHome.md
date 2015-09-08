<h1>SquirrelMQ是一个快速的消息队列。</h1>

**SquirrelMQ特性：**

1. SquirrelMQ使用Slab内存分配算法来降低内存碎片，使用epoll来解决高并发问题。效率比redis要高，使用简单。<br />

2. 另外SquirrelMQ支持持久化，在down机的情况下也不用担心数据丢失。<br />

3. SquirrelMQ支持lua脚本，你可以制定自己的处理队列程序，只要在cron/main.lua中编写代码即可。<br />


<h3>一，SquirrelMQ使用</h3>
下面，我们介绍使用SquirrelMQ消息队列来完成上面所说的应用吧。<br />
1)  安装Lua。<br />

2)  首先下载编译SquirrelMQ：<br />
#> wget http://squirrel-message-queue.googlecode.com/files/squirrel-with-lua-v1.2.zip<br />
#> tar –zxvf squirrel-with-lua-v1.2.zip<br />
#> cd squirrel-with-lua-v1.2<br />
#> make<br />

3) 修改SquirrelMQ配置（squirrel.conf文件）：<br />
```
# 侦听端口
listingPort 6061

# 最大可以使用内存数（单位：字节）
memoryLimitUsed 524288000

# 多长时间进行存储数据到硬盘（防止down机时数据丢失，单位为秒)
secondsToSaveDisk 30

# 多少次数据变化才进行存储数据到硬盘（防止写数据过于频繁）
chagesToSaveDisk 30

# 客户端连接多长时间不操作自动关闭（单位为秒）
clientExpiredTime 60

# 多长时间运行一次cron（单位为毫秒）
cronLoops 5000

# 是否需要密码认证
enableAuth 0

# 认证密码(在enableAuth为1时才需要)
authPwd xn2k@*%bse!@

# lua脚本的路径
luaFilePath /var/squirrelmq/main.lua

# 提供给SquirrelMQ调用的函数
luaMainFunction __main__

# 是否使用守护进程模式运行
daemonize 0
```

我们根据自己的需求来修改配置，特别说明一下的是，当开启Lua处理线程时，我们可以编写Lua脚本来处理队列（在cron/main.lua）。这样就可以让服务器本身来处理消息队列的数据，而不用另外写一个cron程序来处理。下面我们会介绍。<br />

4) 运行SquirrelMQ：<br />
#> ./squirrel –c squirrel.conf<br />

<h3>二，使用客户端API</h3>
SquirrelMQ提供一个PHP访问的API，在php/squirrel.class.php。我们可以使用这个API文件轻松地访问SquirrelMQ。<br />
这个API文件把所有访问SquirrelMQ的操作封装成一个类，叫Squirrel，在使用时直接new一个Squirrel的对象即可，如下：<br />

```
<?php
include("squirrel.class.php");
$smq = new Squirrel('127.0.0.1', 6061);
$smq->push_tail("INSERT INTO mytable(uid, username, password)VALUES(NULL, 'liexusong', '123456');");
?>
```
这样，我们就可以把一条消息插入到消息队列了。我们可以使用size()方法来获取SquirrelMQ的消息条数：<br />
```
<?php
include("squirrel.class.php");
$smq = new Squirrel('127.0.0.1', 6061);
$size = $smq->size();
echo "The SquirrelMQ size: $size";
?>
```

SquirrelMQ支持的API有：<br />
```
1)插入到队列的头部：
$smq->push_head($message);

2)插入到队列的尾部：
$smq->push_tail($message);

3)取得队列的第一条消息，并从队列中删除：
$message = $smq->pop_head();

4)取得队列的最后一条消息，并从队列中删除：
$message = $smq->pop_tail();

5)取得队列的第n条消息，并且从队列中删除：
$message = $smq->pop_index($index);

6)取得队列的第一条消息，但不从队列中删除：
$message = $smq->get_head();

7)取得队列的最后一条消息，但不从队列中删除：
$message = $smq->get_tail();

8)取得队列的第n条消息，但不从队列中删除：
$message = $smq->get_index($index);

9)取得队列的大小：
$size = $smq->size();

10)取得队列的状态：
$stat = $smq->stat();
```

<h3>三，使用Lua处理队列</h3>
SquirrelMQ的一个令人兴奋的特性就是支持使用Lua处理队列中的消息，下面我们来介绍一下这个功能。<br />
要开启Lua处理线程，需要在配置文件中把enableLuaThread设置为1。这样SquirrelMQ就会开启Lua处理线程。我们可以在cron/main.lua文件中编写我们的Lua代码来处理队列中的消息。在cron/main.lua文件中，必须编写一个main的函数，SquirrelMQ就是以这个函数作为入口，如：<br />
```
function __main__()
......
end
```


在main函数中，我们可以使用一些SquirrelMQ提供的API函数取得队列中的消息，如smq\_pop\_head()和smq\_pop\_tail()等。main函数可以这样写：<br />
```
require "luasql.mysql"

function __main__()
    env = luasql.mysql()
    con = env:connect("database", "username", "password", "127.0.0.1", 3306)

    while true do
       local ok, sql = smq_pop_head()
       if ok then
           res = con:execute(sql)
       end
    end

    con:close()
    env:close()
end
```

记住，SquirrelMQ提供的API都是阻塞的，也就是说，当队列为空时，API会阻塞知道队列有消息可以获取为止，这样做的目的是为了尽量减少Lua线程的运行时间。<br />
在上面例子中，我们使用smq\_pop\_head()来取得队列的第一条消息，然后执行此消息（con:execute(sql)）。<br />

SquirrelMQ提供给Lua线程使用的API有：<br />
```
ok, item = smq_pop_head()

ok, item = smq_pop_tail()

ok, item = smq_pop_index()

ok, item = smq_get_head()

ok, item = smq_get_tail()

ok, item = smq_get_index()

size = smq_queue_size()

ok = smq_push_head(item)

ok = smq_push_tail(item)
```

上面的API对应PHP客户端的API。<br />


**有问题可以联系我，新浪微薄：旭松\_Lie**