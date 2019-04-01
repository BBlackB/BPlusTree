/*
 * @file BPlusTree.cc
 * @brief
 * B+树源文件
 *
 * @author Liu GuangRui
 * @email 675040625@qq.com
 */
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include "BPlusTree.h"

BPlusTree::BPlusTree(const char *fileName, int blockSize)
    : fileName_(fileName)
{
    char bootFile[32];
    off_t freeBlock;
    int fd = open(bootFile, O_RDONLY, 0644);

    // 读取配置
    if (fd > 0) {
        printf("load boot file...\n");
        root_ = offsetLoad(fd);
        blockSize_ = offsetLoad(fd);
        fileSize_ = offsetLoad(fd);

        // 加载空闲块
        while ((freeBlock = offsetLoad(fd)) != INVALID_OFFSET) {
            freeBlocks_.push_back(freeBlock);
        }
        close(fd);
    } else {
        root_ = INVALID_OFFSET;
        blockSize_ = blockSize;
        fileSize_ = 0;
    }

    // 计算树的度
    DEGREE = (blockSize_ - sizeof(Node)) / (sizeof(key_t) + sizeof(off_t));
    printf("Degree = %d\n", DEGREE);
    printf("Block size = %ld\n", blockSize_);
    printf("Node = %ld\n", sizeof(Node));

    // 打开索引文件
    fd_ = open(fileName, O_CREAT | O_DIRECT | O_RDWR, 0644);
    assert(fd_ >= 0);

    // chche分配空间
    rootCache_ = (Node *) malloc(blockSize_);
    caches_ = (Node *) malloc(MAX_CACHE_NUM * blockSize_);

    // 若存在root,则读到缓存
    if (root_ != INVALID_OFFSET) {
        int len = pread(fd_, rootCache_, blockSize_, root_);
        assert(len == blockSize_);
    }
}

BPlusTree::~BPlusTree()
{
    char bootFile[32];
    sprintf(bootFile, "%s.boot", fileName_);
    int fd = open(bootFile, O_CREAT | O_WRONLY, 0644);
    assert(fd >= 0);
    // 保存配置
    assert(offsetStore(fd, root_) == S_OK);
    assert(offsetStore(fd, blockSize_) == S_OK);
    assert(offsetStore(fd, fileSize_) == S_OK);

    // 保存空闲块
    for (auto it = freeBlocks_.begin(); it != freeBlocks_.end(); ++it) {
        assert(offsetStore(fd, (*it)) == S_OK);
    }

    free(caches_);

    // 关闭文件
    close(fd);
    close(fd_);
}

void BPlusTree::commandHander()
{
    while (true) {
        printf("Please input your command.(Type 'h' for help):");
        fgets(cmdBuf_, sizeof cmdBuf_, stdin);
        switch (cmdBuf_[0]) {
        case 'h':
            help();
            break;
        case 'i':
            insertHandler();
            break;
        case 'q':
            return;

        default:
            break;
        }
    }
}

void BPlusTree::help()
{
    printf("i: Insert key. e.g. i 1 4-7 9\n");
    printf("r: Remove key. e.g. r 1 4-7 9\n");
    printf("s: Search by key. e.g. s 41-50\n");
    printf("d: Dump the tree structure.\n");
    printf("q: Quit.\n");
}

int BPlusTree::insert(key_t k, long value)
{
    Node *node = rootCache_;
    while (node != NULL) {
        if (isLeaf(node)) {
            insertLeaf(node, k, value);

        } else {
            int pos = searchInNode(node, k);

            if (pos >= 0)
                node = locateNode(subNode(node, pos));
            else // 没有找到,则读取在此范围的block
                node = locateNode(subNode(node, -pos));
        }
    }

    // 新的root节点
    Node *root = newNode();
    key(root)[0] = k;
    data(root)[0] = value;
    root->count = 1;
    root_ = appendBlock(root);
    blockFlush(root);

    return S_OK;
}

off_t BPlusTree::offsetLoad(int fd)
{
    char buf[ADDR_OFFSET_LENTH];
    int len = read(fd, buf, ADDR_OFFSET_LENTH);
    return len > 0 ? pchar_2_off_t(buf, sizeof buf) : INVALID_OFFSET;
}

int BPlusTree::offsetStore(int fd, off_t offset)
{
    char buf[ADDR_OFFSET_LENTH];
    off_t_2_pchar(offset, buf, sizeof buf);
    return write(fd, buf, sizeof buf) == ADDR_OFFSET_LENTH ? S_OK : S_FALSE;
}

int BPlusTree::insertHandler()
{
    char *s = strstr(cmdBuf_, " ");
    int n = atoi(++s);
    // printf("insert %d.\n", n);
    return insert(n, n);
}

Node *BPlusTree::cacheRefer()
{
    // 找到一块空闲的缓存使用
    for (int i = 0; i < MAX_CACHE_NUM; ++i) {
        if (!used_[i]) {
            used_[i] = true;
            return &caches_[i];
        }
    }
    assert(0);
}

void BPlusTree::cacheDefer(const Node *node)
{
    // 找到对应的缓存并释放
    for (int i = 0; i < MAX_CACHE_NUM; ++i) {
        if (node == &caches_[i]) {
            used_[i] = false;
            return;
        }
    }
    assert(0);
}

Node *BPlusTree::newNode()
{
    Node *node = cacheRefer();
    node->self = INVALID_OFFSET;
    node->parent = INVALID_OFFSET;
    node->prev = INVALID_OFFSET;
    node->parent = INVALID_OFFSET;

    node->count = 0;
    return node;
}

Node *BPlusTree::newNonLeaf()
{
    Node *node = newNode();
    node->type = BPLUS_TREE_NON_LEAF;
    return node;
}

Node *BPlusTree::newLeaf()
{
    Node *node = newNode();
    node->type = BPLUS_TREE_LEAF;
    return node;
}

off_t BPlusTree::appendBlock(Node *node)
{
    if (!freeBlocks_.empty()) {
        // 取出空闲块
        node->self = freeBlocks_.front();
        freeBlocks_.pop_front();
    } else {
        // 在文件末尾增加块
        node->self = fileSize_;
        fileSize_ += blockSize_;
    }
    return node->self;
}

int BPlusTree::blockFlush(Node *node)
{
    if (node == NULL) return S_FALSE;

    int ret = pwrite(fd_, node, blockSize_, node->self);
    assert(ret == blockSize_);
    cacheDefer(node);
}

Node *BPlusTree::fetchBlock(off_t offset)
{
    if (offset == INVALID_OFFSET) return NULL;

    Node *node = cacheRefer();
    int len = pread(fd_, node, blockSize_, offset);
    assert(len == blockSize_);

    return node;
}

// 把block读到cache中
Node *BPlusTree::locateNode(off_t offset)
{
    if (offset == INVALID_OFFSET) return NULL;

    // REVIEW:why?
    for (int i = 0; i < MAX_CACHE_NUM; i++) {
        if (!used_[i]) {
            int len = pread(fd_, &caches_[i], blockSize_, offset);
            assert(len == blockSize_);
            return &caches_[i];
        }
    }
    assert(0);
}

int BPlusTree::searchInNode(Node *node, key_t target)
{
    key_t *keys = key(node);
    int low = 0;
    int high = node->count - 1;
    int mid;

    // 若大于此node的最大值,直接返回 NOTE:此时不存在!
    if (target > keys[high]) return high + 1;

    // 二分查找
    while (low < high) {
        mid = (low + high) / 2;
        if (keys[mid] == target) break;

        if (keys[mid] < target)
            low = mid + 1;
        else
            high = mid - 1;
    }

    // 找到则返回位置,否则返回所在范围位置的相反数
    if (keys[mid] == target)
        return high;
    else
        return -high;
}

int BPlusTree::insertLeaf(Node *leaf, key_t k, long value)
{
    int pos = searchInNode(leaf, k);
    if (pos >= 0 || pos != leaf->count) {
        // TODO:插入已存在的点
        return S_FALSE;
    }

    /*新节点*/

    // 恢复正确的节点位置
    if (pos < 0) pos = -pos;

    int ui = (leaf - caches_) / blockSize_;
    used_[ui] = true;

    // block已满->分裂
    if (leaf->count == DEGREE) {
        int split = (DEGREE + 1) / 2;
        Node *anotherNode = newNode();

        if (pos < split) { // TODO:left split
        } else {           // TODO:right split
        }
    } else { // block未满->不分裂
        simpleInsertLeaf(leaf, pos, k, value);
        blockFlush(leaf);
    }

    return S_OK;
}

void BPlusTree::simpleInsertLeaf(Node *leaf, int pos, key_t k, long value)
{
    // 若插入不是在末尾,则需要移动数据
    if (pos < leaf->count) {
        memmove(
            &key(leaf)[pos + 1],
            &key(leaf)[pos],
            (leaf->count - pos) * sizeof(key_t));

        memmove(
            &data(leaf)[pos + 1],
            &data(leaf)[pos],
            (leaf->count - pos) * sizeof(long));
    }

    key(leaf)[pos] = k;
    data(leaf)[pos] = value;
    leaf->count++;
}

int BPlusTree::spiltLeftLeaf(
    Node *leaf,
    Node *left,
    key_t k,
    long value,
    int pos)
{
    int spilt = (DEGREE + 1) / 2;
}

void BPlusTree::addLeftNode(Node *node, Node *left)
{
    // 为left分配物理空间
    appendBlock(left);

    Node *prev = fetchBlock(node->prev);
    if (prev != NULL) {
        prev->next = left->self;
        left->prev = prev->self;
    } else {
        left->prev = INVALID_OFFSET;
    }
    left->next = node->self;
    node->prev = left->self;
}

void BPlusTree::addRightNode(Node *node, Node *right)
{
    appendBlock(right);

    // TODO:同上函数
}

// 字符串转换为off_t
off_t BPlusTree::pchar_2_off_t(const char *str, size_t size)
{
    off_t ret = 0;
    size_t i;
    for (i = 0; i < size; ++i) {
        ret <<= 8;
        ret |= (unsigned char) str[i];
    }
    return ret;
}

// off_t转换为字符串
void BPlusTree::off_t_2_pchar(off_t offset, char *buf, int len)
{
    while (len-- > 0) {
        buf[len] = offset & 0xff;
        offset >>= 8;
    }
}