# SimpleDB_1.1

<p>参考了https://github.com/SIZMW/simpledb-buffer-manager</p>
<p>他实现了buffer调度算法的LRU和Clock算法</p>
<p>在此基础上添加了FIFO，LFU，MRU</p>
<p>这是目前所作的变动</p>

<p>代码的部署运行和原来的simpleDB无异。</p>
<p>Server运行方式</p>
```
java simpledb.server.Startup -lru
java simpledb.server.Startup -fifo
```
<p>Client运行方式:可以运行这个文件，里面是很多sql语句</p>
```
java ExecuteSimpleDBSQL '/home/parallels/Documents/SimpleDB/src/sqlclient/sqlqueries/examples.sql'
```
