# BPlusTree 磁盘实现

Thanks to https://github.com/begeekmyfriend/bplustree

## 区别
- C++实现
- Node结构取消parent,在BPlusTree类中用栈维护父节点
- 非叶子节点和叶子节都可以最多保存DEGREE个key
- Node结构增加一个lastOffset变量用于保存最后一个子节点
- ~~root节点常驻内存~~
- ~~存在相同键值~~
