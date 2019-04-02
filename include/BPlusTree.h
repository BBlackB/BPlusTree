/*
 * @file BPlusTree.h
 * @brief
 * B+树的头文件,主要包含一些定义
 *
 * @author Liu GuangRui
 * @email 675040625@qq.com
 */
#ifndef __BPLUSTREE_H__
#define __BPLUSTREE_H__
#include <list>
#include <unistd.h>

// #define BPTREE_DEGREE 3
#define key_t long
#define data_t long

#define S_OK 0
#define S_FALSE -1

#pragma pack(push)
#pragma pack(2)
struct Node
{
    off_t self; // 当前block在文件中的偏移
    off_t parent;
    off_t prev;
    off_t next;
    off_t lastOffset; // 当此节点为非叶子节点时,lastOffset指向最右的子节点

    short type; // 叶子节点/非叶子节点
    int count;  // 节点中key的个数
};
#pragma pack(pop)

class BPlusTree
{
    // 一些常量
  private:
    static const int ADDR_OFFSET_LENTH = 16; // 每次读取文件的长度
    static const off_t INVALID_OFFSET = 0xDEADBEEF; // 错误的文件偏移量
    static const int MAX_CACHE_NUM = 5;             // 最大缓存块数量
    enum
    {
        BPLUS_TREE_LEAF = 0,
        BPLUS_TREE_NON_LEAF = 1
    };

  private:
    off_t root_;                  // 记录root的偏移量
    off_t blockSize_;             // 块大小
    off_t fileSize_;              // 指向文件末尾,便于创建新的block
    std::list<off_t> freeBlocks_; // 记录空闲块
    std::list<off_t> traceNode_; // 记录经过的父节点(Node结构可省去父指针)
    const char *fileName_;     // 索引文件
    int fd_;                   // 索引文件的描述符
    int DEGREE;                // 一个block中的最大节点数
    char cmdBuf_[64];          // 保存命令字符串
    Node *rootCache_;          // root节点缓存
    Node *caches_;             // 块缓存
    bool used_[MAX_CACHE_NUM]; // 标记使用的缓存

  public:
    BPlusTree(const char *fileName, int blockSize);
    ~BPlusTree();

    // 执行命令
    void commandHander();

  private:
    // 显示帮助信息
    void help();
    // 增加数据
    int insert(key_t key, data_t value);
    // 显示树中所有节点
    int dump();

  private:
    // 获取node中key的位置
    inline key_t *key(Node *node) { return (key_t *) (node + sizeof(Node)); }
    // 获取node中data的位置
    inline data_t *data(Node *node)
    {
        return (data_t *) (node + sizeof(Node)) + DEGREE * sizeof(key_t);
    }
    // 获取node中子节点的位置
    inline off_t *subNode(Node *node, int pos)
    {
        // 最后一个位置的子节点偏移保存在node节点中
        if (pos == node->count) return &node->lastOffset;
        return &((off_t *) (node + sizeof(Node) + DEGREE * sizeof(key_t)))[pos];
    }

    // 判断是否为叶子节点
    inline bool isLeaf(Node *node) { return node->type == BPLUS_TREE_LEAF; }

  private:
    // 读取一个偏移量
    off_t offsetLoad(int fd);
    // 存一个偏移量
    int offsetStore(int fd, off_t offset);
    // 数据插入之前的预处理
    int insertHandler();

    // 占用一个缓存
    Node *cacheRefer();
    // 释放一个缓存
    void cacheDefer(const Node *node);

    /***在内存中命名为node***/
    // 在cache中创建新的节点
    Node *newNode();
    // 在cache中创建新的非叶子节点
    Node *newNonLeaf();
    // 在cache中创建新的叶子节点
    Node *newLeaf();
    // 在节点内部查找
    int searchInNode(Node *node, key_t target);

    /***在磁盘中命名为block***/
    // 增加新的block
    off_t appendBlock(Node *node);
    // block写回磁盘
    int blockFlush(Node *node);
    // 把block取到cache中(不可覆盖)
    Node *fetchBlock(off_t offset);
    // 把block读到cache中(可覆盖)
    Node *locateNode(off_t offset);

    // 插入叶子节点
    int insertLeaf(Node *node, key_t key, data_t value);
    // 简单方式插入叶子节点(不分裂)
    void simpleInsertLeaf(Node *leaf, int pos, key_t k, data_t value);
    // 叶子节点左分裂
    key_t splitLeftLeaf(Node *leaf, Node *left, key_t k, data_t value, int pos);
    // 叶子节点右分裂
    key_t
    splitRightLeaf(Node *leaf, Node *right, key_t k, data_t value, int pos);
    // 增加左叶子节点
    void addLeftNode(Node *node, Node *left);
    // 增加右叶子节点
    void addRightNode(Node *node, Node *right);

    // 更新父节点
    int updateParentNode(Node *leftChild, Node *rightChild, key_t k);

    // 在非叶子节点中插入
    int insertNonLeaf(Node *node, Node *leftChild, Node *rightChild, key_t k);
    // 简单方式插入非叶子节点
    void simpleInsertNonLeaf(
        Node *node,
        int pos,
        key_t k,
        Node *leftChild,
        Node *rightChild);

  private:
    // 字符串转换为off_t
    off_t pchar_2_off_t(const char *str, size_t size);
    // off_t转换为字符串
    void off_t_2_pchar(off_t offset, char *buf, int len);
};

#endif // __BPLUSTREE_H__