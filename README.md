# CppZooKeeperApi
A C++ ZooKeeper API wrapper

# Build(Linux)
git clone https://github.com/godmoon/CppZooKeeperApi<br>
cd CppZooKeeperApi<br>
chmod +x ./make_dep.sh<br>
./make_dep.sh<br>
make<br>
<br>
You will get two library files: libCppZooKeeper.so and libCppZooKeeper.a

# 中文资料
Zookeeper C语言API封装和注意事项：http://godmoon.wicp.net/blog/index.php/post_255.html<br>
那些年，我们一起踩过的ZooKeeper的坑：http://godmoon.wicp.net/blog/index.php/post_421.html

# 补充

- 需要下载zookeeper源码并修改、编译
- 将zookeeper生成的.a/.so文件放到makefile中指定的目录下
- ./zookeeper目录为zookeeper c api源码中的头文件
