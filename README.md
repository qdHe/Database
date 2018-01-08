# SimpleDB Extension

### Yancen
----------------

<p>参考了https://github.com/SIZMW/simpledb-buffer-manager</p>
<p>他实现了buffer调度算法的LRU和Clock算法</p>
<p>在此基础上添加了FIFO，LFU，MRU</p>
<p>这是目前所作的变动</p>
<p>代码的部署运行和原来的simpleDB无异。</p>
<p>Server运行方式</p>
<pre><code>java simpledb.server.Startup -lru
java simpledb.server.Startup -fifo
</code></pre>
<p>Client运行方式:可以运行这个文件，里面是很多sql语句</p>
<pre><code>java ExecuteSimpleDBSQL '~/SimpleDB/src/sqlclient/sqlqueries/examples.sql'
</code></pre>
<p>javadoc,bugs.txt,cs4432.log,design.txt以及testing.txt并没有什么用，可以不用看</p>
<p>btw：运行的时候，会在simpledb的母文件夹中生成对应buffer调用方法的log文件，比如cs4432_lru.log</p>

### Qidu
----------------
1. 增加了select语句的筛选条件，原SimpleDB只实现了等于比较，扩增到不等，大于和小于
> **select** sname, grade **from** students **where** grade > 90

2. 支持select语句中 * 

3. 增加了查询语句中fields的range,即多个表格中允许重名，数据库会自动添加前缀来区分

4. 补充了两个优化过的Planner, 参考
https://github.com/afrasiabi/SimpleDB/tree/master/simpledb/myplanner

### Di
----------------
1. 增加了select语句的Order by
> **select** sname, grade **from** students **order by** grade
