# BPlusTree 磁盘实现

Thanks to https://github.com/begeekmyfriend/bplustree

## 区别
- C++实现
- Node结构取消parent,在BPlusTree类中用栈维护父节点
- 非叶子节点和叶子节都可以最多保存DEGREE个key
- Node结构增加一个lastOffset变量用于保存最后一个子节点
- ~~root节点常驻内存~~
- ~~存在相同键值~~

## 性能对比
1. Block size = 128

- bplustree度为5(非叶子节点最多容纳4个,叶子节点最多容纳5个)
- BPlusTree度为5(非叶子节点最多容纳5个,叶子节点最多容纳5个)


- 顺序插入/删除/输出一百万个数耗时(手工计时,存在误差)

|| insert| remove|dump|file size
:--:|:--:|:--:|:--:|:--:|
bplustree|7.46s|8.88s|21.76s|55M/6.8M
BPlusTree|9.36s|9.45s|39.07s|62M/7.7M|