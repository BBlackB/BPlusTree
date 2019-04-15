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
    // off_t parent;   // 可以不用
    off_t prev; // 叶子节点的前一个
    off_t next; // 叶子节点的后一个
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
    enum
    {
        LEFT_NODE = 0,
        RIGHT_NODE = 1
    };

  private:
    off_t root_;                  // 记录root的偏移量
    off_t blockSize_;             // 块大小
    off_t fileSize_;              // 指向文件末尾,便于创建新的block
    std::list<off_t> freeBlocks_; // 记录空闲块
    std::list<off_t> traceNode_; // 记录经过的父节点(Node结构可省去父指针)
    const char *fileName_; // 索引文件
    int fd_;               // 索引文件的描述符
    int DEGREE;            // 一个block中的最大节点数 NOTE: DEGREE >= 3
    char cmdBuf_[64];      // 保存命令字符串
    Node *rootCache_;      // root节点缓存
    Node *caches_[MAX_CACHE_NUM]; // 块缓存
    bool used_[MAX_CACHE_NUM];    // 标记使用的缓存

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
    // 查找
    long search(key_t k);
    // 删除
    int remove(key_t);
    // 显示树中所有节点
    void dump();

  private:
    // NOTE:指针运算必须转换为char *
    // 获取node中key的位置
    inline key_t *key(const Node *node)
    {
        return (key_t *) ((char *) node + sizeof(Node));
    }
    // 获取node中data的位置
    inline data_t *data(const Node *node)
    {
        return (data_t *) ((char *) key(node) + DEGREE * sizeof(key_t));
    }
    // 获取node中子节点的位置
    inline off_t *subNode(Node *node, const int pos)
    {
        // 最后一个位置的子节点偏移保存在lastOffset中
        if (pos == DEGREE) return &node->lastOffset;
        return &((off_t *) ((char *) key(node) + DEGREE * sizeof(key_t)))[pos];
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
    // 查找数据的预处理
    int searchHandler();
    // 删除之前的数据预处理
    int removeHandler();

    // 占用一个缓存
    Node *cacheRefer();
    // 对node使用缓存占用
    void cacheOccupy(Node *node);
    // 释放一个缓存
    void cacheDefer(const Node *node);

    /***在内存中命名为node***/
    // 在rootCache中创建新的root
    // 使用思路:在locateNode和blockFlush中判断,but,traceNode_?
    Node *newRoot();
    // 创建叶子节点root
    Node *newLeafRoot();
    // 创建非叶子节点root
    Node *newNonLeafRoot();
    // 在cache中创建新的节点
    Node *newNode();
    // 在cache中创建新的非叶子节点
    Node *newNonLeaf();
    // 在cache中创建新的叶子节点
    Node *newLeaf();
    // 在节点内部查找
    int searchInNode(Node *node, key_t target);

    /***在磁盘中命名为block***/
    // 为node节点分配磁盘空间
    off_t appendBlock(Node *node);
    // 回收磁盘空间
    void unappendBlock(Node *node);
    // block写回磁盘
    int blockFlush(Node *node);
    // 把root读取到rootCache_中
    void fetchRootBlock();
    // 把block取到cache中(不可覆盖)
    Node *fetchBlock(off_t offset);
    // 把block读到cache中(可覆盖)
    Node *locateNode(off_t offset);

    /*** Insert ***/
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

    // 为非叶子节点分配磁盘空间
    void addNonLeafNode(Node *anotherNode);
    // 在非叶子节点中插入
    int insertNonLeaf(Node *node, Node *leftChild, Node *rightChild, key_t k);
    // 简单方式插入非叶子节点
    void simpleInsertNonLeaf(
        Node *node,
        int pos,
        key_t k,
        Node *leftChild,
        Node *rightChild);
    // 非叶子节点左分裂(关注lastOffset)
    key_t splitLeftNonLeaf(
        Node *node,
        Node *leftNode,
        int pos,
        key_t k,
        Node *leftChild,
        Node *rightChild);
    // 非叶子节点右分裂1(pos == split)
    key_t splitRightNonLeaf1(
        Node *node,
        Node *rightNode,
        int pos,
        key_t k,
        Node *leftChild,
        Node *rightChild);
    // 非叶子节点右分裂(pos > split)
    key_t splitRightNonLeaf2(
        Node *node,
        Node *rightNode,
        int pos,
        key_t k,
        Node *leftChild,
        Node *rightChild);

    /*** Remove ***/
    // 删除节点,回收block
    void removeNode(Node *node, Node *left, Node *right);
    // 删除叶子节点(与其他叶子节点合并)
    int removeLeaf(Node *node, key_t k);
    // 简单删除叶子节点
    void simpleRemoveInLeaf(Node *node, int pos);
    // 选择使用左节点还是右节点来借数据
    int selectNode(Node *node, Node *left, Node *right, int pos);
    // 从left转移一位数据到node
    void
    shiftLeafFromLeft(Node *node, Node *left, Node *parent, int ppos, int pos);
    // node合并到left
    void mergeLeafIntoLeft(Node *node, Node *left, int pos);
    // 从right转移一位到node
    void shiftLeafFromRight(
        Node *node,
        Node *right,
        Node *parent,
        int ppos,
        int pos);
    // right合并到node
    void mergeLeafWithRight(Node *node, Node *right);
    // 删除非叶子节点的key
    void removeInNonLeaf(Node *node, int pos);
    // 简单删除非叶子节点
    void simpleRemoveInNonLeaf(Node *node, int pos);
    // 非叶子节 向左移动一位 left->parent parent->node
    void shiftNonLeafFromLeft(
        Node *node,
        Node *left,
        Node *parent,
        int ppos,
        int pos);
    // node合并到left parent->left node->left
    void mergeNonLeafIntoLeft(
        Node *node,
        Node *left,
        Node *parent,
        int ppos,
        int pos);
    // 非叶子节点 向左移动一位 parent->node right->parent
    void shiftNonLeafFromRight(
        Node *node,
        Node *right,
        Node *parent,
        int ppos,
        int pos);
    // right合并到node parent->node right->node
    void mergeNonLeafWithRight(
        Node *node,
        Node *right,
        Node *parent,
        int ppos,
        int pos);

  private:
    // 字符串转换为off_t
    off_t pchar_2_off_t(const char *str, size_t size);
    // off_t转换为字符串
    void off_t_2_pchar(off_t offset, char *buf, int len);

    // 输出当前节点
    void draw(Node *node, int level);

    // 打印所有叶子节点.  测试用
    void showLeaves();
};

#endif // __BPLUSTREE_H__