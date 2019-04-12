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
#include <errno.h>
#include "BPlusTree.h"

BPlusTree::BPlusTree(const char *fileName, int blockSize)
    : fileName_(fileName)
{
    char bootFile[32];
    off_t freeBlock;
    sprintf(bootFile, "%s.boot", fileName);
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
    assert(DEGREE > 2);

    printf("Degree = %d\n", DEGREE);
    printf("Block size = %ld\n", blockSize_);
    printf("Node = %ld\n", sizeof(Node));

    /**
     * 使用O_DIRECT需要遵守的一些限制：
     * 用于传递数据的缓冲区，其内存边界必须对齐为块大小的整数倍
     * 数据传输的开始点，即文件和设备的偏移量，必须是块大小的整数倍
     * 待传递数据的长度必须是块大小的整数倍。
     * 不遵守上述任一限制均将导致EINVAL错误。
     */
    // 打开索引文件 FIXME:暂未打开O_DIRECT
    fd_ = open(fileName, O_CREAT | O_RDWR, 0644);
    assert(fd_ >= 0);

    // chche分配空间
    rootCache_ = (Node *) malloc(blockSize_);
    for (int i = 0; i < MAX_CACHE_NUM; i++) {
        caches_[i] = (Node *) malloc(blockSize_);
    }

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

    for (int i = 0; i < MAX_CACHE_NUM; ++i)
        free(caches_[i]);

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
        case 'd':
            dump();
            break;
        case 's':
            searchHandler();
            break;
        case 'r':
            removeHandler();
            break;
        case 't':
            showLeaves();
            break;

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

int BPlusTree::insert(key_t k, data_t value)
{
    // FIXME:如何使用root_
    // Node *node = rootCache_;
    Node *node = locateNode(root_);

    // 清空traceNode_
    traceNode_.clear();

    while (node != NULL) {
        if (isLeaf(node)) {
            return insertLeaf(node, k, value);

        } else {
            // 记录父节点偏移
            traceNode_.push_back(node->self);

            int pos = searchInNode(node, k);
            if (pos >= 0)
                node = locateNode(*subNode(node, pos + 1));
            else // 没有找到,则读取在此范围的block
                node = locateNode(*subNode(node, -pos - 1));
        }
    }

    // 新的root节点
    Node *root = newLeaf();
    key(root)[0] = k;
    data(root)[0] = value;
    root->count = 1;
    root_ = appendBlock(root);

    blockFlush(root);

    return S_OK;
}

// data_t == long
long BPlusTree::search(key_t k)
{
    long ret = -1;
    Node *node = locateNode(root_);

    while (NULL != node) {
        int pos = searchInNode(node, k);
        if (isLeaf(node)) {
            ret = pos >= 0 ? data(node)[pos] : -1;
            break;
        } else {
            if (pos >= 0)
                node = locateNode(*subNode(node, pos + 1));
            else
                node = locateNode(*subNode(node, -pos - 1));
        }
    }
    return ret;
}

int BPlusTree::remove(key_t k)
{
    Node *node = locateNode(root_);

    // 清空traceNode_
    traceNode_.clear();

    while (node != NULL) {
        if (isLeaf(node)) {
            return removeLeaf(node, k);
        } else {
            // 记录经过的节点
            traceNode_.push_back(node->self);

            int pos = searchInNode(node, k);
            if (pos >= 0)
                node = locateNode(*subNode(node, pos + 1));
            else
                node = locateNode(*subNode(node, -pos - 1));
        }
    }

    // 没找到,则返回-1
    return -1;
}

void BPlusTree::draw(Node *node, int level)
{
    if (level != 0) {
        for (int i = 0; i < level - 1; i++)
            printf("%-8s", " ");
        printf("%-8s", "+-------");
    }

    //
    if (isLeaf(node))
        printf("leaf:");
    else
        printf("node:");

    for (int i = 0; i < node->count; i++)
        printf(" %ld", key(node)[i]);
    printf("\n");
}

// 显示树中所有节点 前序遍历
void BPlusTree::dump()
{
    struct NodeInfo
    {
        off_t offset;
        int level;
    };

    if (root_ == INVALID_OFFSET) return;
    // stack
    std::list<NodeInfo> preOrderStack;

    preOrderStack.push_back(NodeInfo{root_, 0});

    while (!preOrderStack.empty()) {
        NodeInfo nodeinfo = preOrderStack.back();
        preOrderStack.pop_back();

        Node *node = locateNode(nodeinfo.offset);

        draw(node, nodeinfo.level);

        if (!isLeaf(node)) {
            // 把所有子节点按倒序压栈
            for (int i = node->count; i >= 0; i--) {
                preOrderStack.push_back(
                    NodeInfo{*subNode(node, i), nodeinfo.level + 1});
            }
        }
    }
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

// key_t == long
int BPlusTree::insertHandler()
{
    char *s = strstr(cmdBuf_, " ");
    if (s == NULL) goto faild;

    s++;
    if (*s >= '0' && *s <= '9') {
        char *s2 = strstr(s, "-");
        // 插入区间
        if (s2 != NULL) {
            key_t n1, n2;
            sscanf(s, "%ld-%ld", &n1, &n2);
            for (; n1 <= n2; n1++)
                insert(n1, n1);
            return S_OK;
        } else { // 插入一个数
            key_t n = atoi(s);
            return insert(n, n);
        }
    }

faild:
    printf("Invalid argument.\n");
    return S_FALSE;
}

// key_t == long && data_t == long
int BPlusTree::searchHandler()
{
    char *s = strstr(cmdBuf_, " ");
    if (NULL == s) goto faild;

    s++;

    if (*s >= '0' && *s <= '9') {
        char *s2 = strstr(s, "-");
        // 查找区间
        if (s2 != NULL) {
            key_t n1, n2;
            sscanf(s, "%ld-%ld", &n1, &n2);
            for (; n1 <= n2; n1++)
                printf("key: %ld, index: %ld\n", n1, search(n1));
            return S_OK;
        } else { // 查找一个数
            key_t n = atoi(s);
            printf("key: %ld, index: %ld\n", n, search(n));
            return S_OK;
        }

        return S_OK;
    }

faild:
    printf("Invalid argument.\n");
    return S_FALSE;
}

int BPlusTree::removeHandler()
{
    char *s = strstr(cmdBuf_, " ");
    if (s == NULL) goto faild;

    s++;
    if (*s >= '0' && *s <= '9') {
        char *s2 = strstr(s, "-");
        // 删除区间
        if (s2 != NULL) {
            key_t n1, n2;
            sscanf(s, "%ld-%ld", &n1, &n2);
            for (; n1 <= n2; n1++)
                remove(n1);
            return S_OK;
        } else { // 删除一个数
            key_t n = atoi(s);
            return remove(n);
        }
    }

faild:
    printf("Invalid argument.\n");
    return S_FALSE;
}

Node *BPlusTree::cacheRefer()
{
    // 找到一块空闲的缓存使用
    for (int i = 0; i < MAX_CACHE_NUM; ++i) {
        if (!used_[i]) {
            used_[i] = true;
            return caches_[i];
        }
    }
    assert(0);
}

void BPlusTree::cacheDefer(const Node *node)
{
    // 找到对应的缓存并释放
    for (int i = 0; i < MAX_CACHE_NUM; ++i) {
        if (node == caches_[i]) {
            used_[i] = false;
            return;
        }
    }
    assert(0);
}

Node *BPlusTree::newRoot()
{
    rootCache_->self = INVALID_OFFSET;
    rootCache_->prev = INVALID_OFFSET;
    rootCache_->next = INVALID_OFFSET;
    rootCache_->lastOffset = INVALID_OFFSET;

    return rootCache_;
}

Node *BPlusTree::newNode()
{
    Node *node = cacheRefer();
    node->self = INVALID_OFFSET;
    // node->parent = INVALID_OFFSET;
    node->prev = INVALID_OFFSET;
    node->next = INVALID_OFFSET;
    node->lastOffset = INVALID_OFFSET;

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

void BPlusTree::unappendBlock(Node *node)
{
    // 若回收最后一个block,则直接减少fileSize_
    if (fileSize_ - blockSize_ == node->self)
        fileSize_ -= blockSize_;
    else // 否则使用链表回收
        freeBlocks_.push_back(node->self);
}

int BPlusTree::blockFlush(Node *node)
{
    if (node == NULL) return S_FALSE;

    int ret = pwrite(fd_, node, blockSize_, node->self);
    if (ret == -1) { printf("strerror: %s\n", strerror(errno)); }
    assert(ret == blockSize_);
    cacheDefer(node);

    return S_OK;
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
            int len = pread(fd_, caches_[i], blockSize_, offset);
            assert(len == blockSize_);
            return caches_[i];
        }
    }
    assert(0);
}

// 返回值: 非负数->存在  负数->可插入坐标的相反数减1
int BPlusTree::searchInNode(Node *node, key_t target)
{
    key_t *keys = key(node);
    int low = 0;
    int high = node->count - 1;
    int mid;

    // 二分查找
    while (low < high) {
        mid = (low + high) / 2;
        // 找到则直接返回对应坐标
        if (keys[mid] == target) return mid;

        if (keys[mid] < target)
            low = mid + 1;
        else
            high = mid - 1;
    }

    // high == -1
    if (high < 0) return high;

    // 返回第一个大于target的坐标的相反数减1(避免0的双意性)
    if (keys[high] > target)
        return -high - 1;
    else if (keys[high] == target) // 找到则返回
        return high;
    else
        return -high - 2;
}

int BPlusTree::insertLeaf(Node *leaf, key_t k, data_t value)
{
    int pos = searchInNode(leaf, k);
    if (pos >= 0) {
        // TODO:插入已存在的点
        assert(0);
    }

    /*新节点*/

    // 恢复正确的节点位置
    pos = -pos - 1;

    int i = 0;
    while (leaf != caches_[i++])
        ;
    used_[--i] = true;

    // block已满->分裂
    if (leaf->count == DEGREE) {
        int split = (DEGREE + 1) / 2;
        // NOTE:another何时写回
        Node *anotherNode = newLeaf();
        key_t splitkey;

        if (pos < split) { // 分裂出左叶子
            splitkey = splitLeftLeaf(leaf, anotherNode, k, value, pos);
        } else { // 分裂出右叶子
            splitkey = splitRightLeaf(leaf, anotherNode, k, value, pos);
        }

        // 递归维护上层节点
        if (pos < split)
            updateParentNode(anotherNode, leaf, splitkey);
        else
            updateParentNode(leaf, anotherNode, splitkey);

    } else { // block未满->不分裂
        simpleInsertLeaf(leaf, pos, k, value);
        blockFlush(leaf);
    }

    return S_OK;
}

void BPlusTree::simpleInsertLeaf(Node *leaf, int pos, key_t k, data_t value)
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
            (leaf->count - pos) * sizeof(data_t));
    }

    key(leaf)[pos] = k;
    data(leaf)[pos] = value;
    leaf->count++;
}

key_t BPlusTree::splitLeftLeaf(
    Node *leaf,
    Node *left,
    key_t k,
    data_t value,
    int pos)
{
    // 从中间位置开始分裂
    int split = (DEGREE + 1) / 2;

    // 增加左叶子节点
    addLeftNode(leaf, left);

    left->count = split;
    leaf->count = DEGREE - split + 1;

    // 移动数据到left. pos + 1 + (split - pos - 1) == split == left->count
    if (pos != 0) {
        memmove(&key(left)[0], &key(leaf)[0], pos * sizeof(key_t));
        memmove(&data(left)[0], &data(leaf)[0], pos * sizeof(data_t));
    }

    key(left)[pos] = k;
    data(left)[pos] = value;

    // 继续移动剩余数据left
    memmove(
        &key(left)[pos + 1],
        &key(leaf)[pos],
        (split - pos - 1) * sizeof(key_t));
    memmove(
        &data(left)[pos + 1],
        &data(leaf)[pos],
        (split - pos - 1) * sizeof(data_t));

    // 移动leaf数据
    memmove(&key(leaf)[0], &key(leaf)[split - 1], leaf->count * sizeof(key_t));
    memmove(
        &data(leaf)[0], &data(leaf)[split - 1], leaf->count * sizeof(data_t));

    // NOTE:叶子节点无需考虑lastOffset
    // 分裂完成后,返回右节点的第一个key
    return key(leaf)[0];
}

key_t BPlusTree::splitRightLeaf(
    Node *leaf,
    Node *right,
    key_t k,
    data_t value,
    int pos)
{
    int split = (DEGREE + 1) / 2;

    addRightNode(leaf, right);

    leaf->count = split;
    right->count = DEGREE - split + 1;

    // (pos - split) + 1 + (DEGREE - pos) == right->count
    if (pos != split) {
        memmove(
            &key(right)[0], &key(leaf)[split], (pos - split) * sizeof(key_t));
        memmove(
            &data(right)[0],
            &data(leaf)[split],
            (pos - split) * sizeof(data_t));
    }

    key(right)[pos - split] = k;
    data(right)[pos - split] = value;

    if (pos != right->count) {
        memmove(
            &key(right)[pos - split + 1],
            &key(leaf)[pos],
            (DEGREE - pos) * sizeof(key_t));
        memmove(
            &data(right)[pos - split + 1],
            &data(leaf)[pos],
            (DEGREE - pos) * sizeof(data_t));
    }

    return key(right)[0];
}

void BPlusTree::addLeftNode(Node *node, Node *left)
{
    // 为left分配物理空间
    appendBlock(left);

    Node *prev = fetchBlock(node->prev);
    if (prev != NULL) {
        prev->next = left->self;
        left->prev = prev->self;
        blockFlush(prev); // 写回磁盘
    } else {
        left->prev = INVALID_OFFSET;
    }
    left->next = node->self;
    node->prev = left->self;
}

void BPlusTree::addRightNode(Node *node, Node *right)
{
    appendBlock(right);

    Node *next = fetchBlock(node->next);
    if (next != NULL) {
        next->prev = right->self;
        right->next = next->self;
        blockFlush(next); // 写回磁盘
    } else {
        right->next = INVALID_OFFSET;
    }
    right->prev = node->self;
    node->next = right->self;
}

// 更新父节点
int BPlusTree::updateParentNode(Node *leftChild, Node *rightChild, key_t k)
{
    // 没有父节点
    if (traceNode_.empty()) {
        // 创建父节点
        Node *parent = newNonLeaf();
        key(parent)[0] = k;
        parent->count = 1;
        *subNode(parent, 0) = leftChild->self;
        *subNode(parent, 1) = rightChild->self;

        // 设置root
        root_ = appendBlock(parent);

        // 全部刷进磁盘
        blockFlush(leftChild);
        blockFlush(rightChild);
        blockFlush(parent);
    } else {
        // 取出父节点并增加一个key
        off_t p = traceNode_.back();
        traceNode_.pop_back();

        // 在非叶子节点中插入key
        return insertNonLeaf(fetchBlock(p), leftChild, rightChild, k);
    }

    return S_OK;
}

void BPlusTree::addNonLeafNode(Node *anotherNode) { appendBlock(anotherNode); }

int BPlusTree::insertNonLeaf(
    Node *node,
    Node *leftChild,
    Node *rightChild,
    key_t k)
{
    int pos = searchInNode(node, k);
    // TODO:暂不支持相同key
    // if (pos < 0 && pos > node->count) assert(0);
    assert(pos < 0);
    if (pos < 0) pos = -pos - 1;

    // 该节点已满,需关注lastOffset
    if (node->count == DEGREE) {
        int split = DEGREE / 2;
        key_t splitkey;
        Node *anotherNode = newNonLeaf();

        // 分情况插入
        if (pos < split) {
            splitkey = splitLeftNonLeaf(
                node, anotherNode, pos, k, leftChild, rightChild);
        } else if (pos == split) {
            splitkey = splitRightNonLeaf1(
                node, anotherNode, pos, k, leftChild, rightChild);
        } else {
            splitkey = splitRightNonLeaf2(
                node, anotherNode, pos, k, leftChild, rightChild);
        }

        // 递归维护上层节点
        if (pos < split)
            updateParentNode(anotherNode, node, splitkey);
        else
            updateParentNode(node, anotherNode, splitkey);

    } else {
        // 该节点未满,直接简单插入
        simpleInsertNonLeaf(node, pos, k, leftChild, rightChild);
        blockFlush(node);
    }

    return S_OK;
}

void BPlusTree::simpleInsertNonLeaf(
    Node *node,
    int pos,
    key_t k,
    Node *leftChild,
    Node *rightChild)
{
    // 插入后节点没有填满(不使用lastOffset)
    if (DEGREE != node->count + 1) {
        // 若在已有的最后插入,则不需要移动数据
        if (pos != node->count) {
            memmove(
                &key(node)[pos + 1],
                &key(node)[pos],
                (node->count - pos) * sizeof(key_t));
            memmove(
                subNode(node, pos + 2),
                subNode(node, pos + 1),
                (node->count - pos) * sizeof(off_t));
        }

    } else { // 插入后节点被填满,需要维护lastOffset
        // 若插入点不是在最后一个位置,则lastOffset由当前最后的点确定
        if (pos != DEGREE - 1) {
            node->lastOffset = *subNode(node, DEGREE - 1);

            memmove(
                &key(node)[pos + 1],
                &key(node)[pos],
                (node->count - pos) * sizeof(key_t));

            // 拷贝数-1,因为最后已放在lastOffset
            memmove(
                subNode(node, pos + 2),
                subNode(node, pos + 1),
                (node->count - pos - 1) * sizeof(off_t));
        }

        // 若插入点在最后一个位置,不需要移动数据,lastOffset由插入点确定
        // 在下方subNode(node, pos+1)更新
    }

    // 对插入点更新
    key(node)[pos] = k;
    *subNode(node, pos) = leftChild->self;
    *subNode(node, pos + 1) = rightChild->self;

    // 把左右节点刷回磁盘
    blockFlush(leftChild);
    blockFlush(rightChild);

    node->count++;
}

// 非叶子节点左分裂(pos < split)(关注lastOffset)
key_t BPlusTree::splitLeftNonLeaf(
    Node *node,
    Node *leftNode,
    int pos,
    key_t k,
    Node *leftChild,
    Node *rightChild)
{
    key_t splitkey;         // 向上层节点增加的key
    int split = DEGREE / 2; // 左边节点个数

    // 非叶子节点无需维护prev/next指针
    addNonLeafNode(leftNode);

    leftNode->count = split;
    node->count = DEGREE - split;

    // leftNode总共有 pos + (split - pos - 1) + 1 == split
    if (pos != 0) {
        memmove(&key(leftNode)[0], &key(node)[0], pos * sizeof(key_t));
        memmove(subNode(leftNode, 0), subNode(node, 0), pos * sizeof(off_t));
    }

    // subNode多拷贝一个
    memmove(
        &key(leftNode)[pos + 1],
        &key(node)[pos],
        (split - pos - 1) * sizeof(key_t));
    memmove(
        subNode(leftNode, pos + 1),
        subNode(node, pos),
        (split - pos) * sizeof(off_t));

    key(leftNode)[pos] = k;

    if (pos == split - 1) {
        *subNode(leftNode, pos) = leftChild->self;
        *subNode(node, 0) = rightChild->self;
    } else {
        *subNode(leftNode, pos) = leftChild->self;
        *subNode(leftNode, pos + 1) = rightChild->self;

        *subNode(node, 0) = *subNode(node, split);
    }

    // 返回split-1位置的key
    splitkey = key(node)[split - 1];

    // 将leftChild和rightChild刷回磁盘
    blockFlush(leftChild);
    blockFlush(rightChild);

    // DEGREE - split == node->count
    memmove(&key(node)[0], &key(node)[split], (DEGREE - split) * sizeof(key_t));
    memmove(
        subNode(node, 1),
        subNode(node, split + 1),
        (DEGREE - split - 1) * sizeof(off_t));

    // node节点个数没有满,则不适用lastOffset
    *subNode(node, DEGREE - split) = node->lastOffset;
    node->lastOffset = INVALID_OFFSET;

    return splitkey;
}

// 非叶子节点右分裂1(pos == split)(将k添加到父节点)
key_t BPlusTree::splitRightNonLeaf1(
    Node *node,
    Node *rightNode,
    int pos,
    key_t k,
    Node *leftChild,
    Node *rightChild)
{
    addNonLeafNode(rightNode);

    node->count = pos;
    rightNode->count = DEGREE - pos;

    // 将DEGREE - pos个key移动到右节点上
    memmove(
        &key(rightNode)[0], &key(node)[pos], rightNode->count * sizeof(key_t));

    // 右节点个数必大于2
    assert(rightNode->count > 2);

    memmove(
        subNode(rightNode, 1),
        subNode(node, pos + 1),
        (rightNode->count - 2) * sizeof(off_t));

    // 左右子节点
    *subNode(node, pos) = leftChild->self;
    *subNode(rightNode, 0) = rightChild->self;

    // 右节点不满,则不用lastOffset
    *subNode(rightNode, rightNode->count) = node->lastOffset;

    // 将左右子节点刷回磁盘
    blockFlush(leftChild);
    blockFlush(rightChild);

    // 返回插入节点的key,用于增加到上层节点中
    return k;
}

// 非叶子节点右分裂(pos > split)
key_t BPlusTree::splitRightNonLeaf2(
    Node *node,
    Node *rightNode,
    int pos,
    key_t k,
    Node *leftChild,
    Node *rightChild)
{
    int split = DEGREE / 2;         // 分裂节点在左节点的位置
    int rightPos = pos - split - 1; // 插入节点在右节点的位置

    // 为新节点分配磁盘空间
    appendBlock(rightNode);

    node->count = split;
    rightNode->count = DEGREE - split;

    // (rightPos) + (DEGREE - pos) + 1 == rightNode->count
    if (rightPos != 0) {
        memmove(
            &key(rightNode)[0],
            &key(node)[split + 1],
            (rightPos) * sizeof(key_t));
        memmove(
            subNode(rightNode, 0),
            subNode(node, split + 1),
            (rightPos) * sizeof(off_t));
    }

    memmove(
        &key(rightNode)[rightPos + 1],
        &key(node)[pos],
        (DEGREE - pos) * sizeof(key_t));
    if (pos < DEGREE - 1)
        memmove(
            subNode(rightNode, rightPos + 2),
            subNode(node, pos + 1),
            (DEGREE - pos - 1) * sizeof(off_t));
    *subNode(rightNode, rightNode->count) = node->lastOffset;

    // 处理新插入节点的key和subNode
    key(rightNode)[rightPos] = k;
    *subNode(rightNode, rightPos) = leftChild->self;
    *subNode(rightNode, rightPos + 1) = rightChild->self;

    // 将左右子节点刷回磁盘
    blockFlush(leftChild);
    blockFlush(rightChild);

    return key(node)[split];
}

void BPlusTree::removeNode(Node *node, Node *left, Node *right)
{
    // 处理叶子节点的前后关系
    if (isLeaf(node)) {
        if (left != NULL) {
            if (right != NULL) {
                left->next = right->self;
                right->prev = left->self;

                blockFlush(right);
            } else
                left->next = INVALID_OFFSET;
            blockFlush(left);
        } else {
            if (right != NULL) {
                right->prev = INVALID_OFFSET;
                blockFlush(right);
            }
        }
    }

    assert(node->self != INVALID_OFFSET);

    // 回收block
    unappendBlock(node);
    cacheDefer(node);
}

int BPlusTree::removeLeaf(Node *node, key_t k)
{
    // FIXME:some bugs
    int pos = searchInNode(node, k);

    // 不存在
    if (pos < 0) return S_FALSE;

    {
        int i = 0;
        while (caches_[i++] != node)
            ;
        used_[--i] = true;
    }

    // 没有父节点,即当前节点为root
    if (traceNode_.empty()) {
        // 只有一个成员,清空树
        if (node->count == 1) {
            // 删除成员必为剩余节点,只需标记root_,无需写回
            assert(k == key(node)[0]);
            root_ = INVALID_OFFSET;
            removeNode(node, NULL, NULL);
        } else { // 删除一个成员
            simpleRemoveInLeaf(node, pos);
            blockFlush(node);
        }
    } // node会与其他节点合并或借数据
    else if (node->count <= (DEGREE + 1) / 2) {
        // 分情况讨论删除

        // 取出该节点的左右节点和父节点
        Node *parent = fetchBlock(traceNode_.back());
        Node *left = fetchBlock(node->prev);
        Node *right = fetchBlock(node->next);

        //  父节点出栈
        traceNode_.pop_back();

        // ppos为node在parent的位置
        int ppos = searchInNode(parent, k);
        if (ppos < 0) ppos = -ppos - 2;

        // 根据左右节点情况来选择
        if (selectNode(parent, left, right, ppos) == LEFT_NODE) {
            // 若left右足够多的数据,则分一个给node
            if (left->count > (DEGREE + 1) / 2) {
                // 从left转移一位数据到node
                shiftLeafFromLeft(node, left, parent, ppos, pos);

                // 把更改刷回磁盘
                blockFlush(parent);
                blockFlush(node);
                blockFlush(left);
                blockFlush(right);
            } else {
                // node合并到left
                mergeLeafIntoLeft(node, left, pos);
                // 删除node节点
                removeNode(node, left, right);
                // 删除父节点的key,并向上更新
                removeInNonLeaf(parent, ppos);
            }
        } else // 选择right节点 NOTE:ppos 有可能为-1
        {
            // 先删除node节点中的k
            simpleRemoveInLeaf(node, pos);

            // 若right节点有足够多的数据,分一个给node
            if (right->count > (DEGREE + 1) / 2) {
                shiftLeafFromRight(node, right, parent, ppos + 1, pos);

                // 把更改刷回磁盘
                blockFlush(parent);
                blockFlush(node);
                blockFlush(left);
                blockFlush(right);
            } else { // right合并到node
                mergeLeafWithRight(node, right);
                // 删除right节点
                removeNode(right, node, fetchBlock(right->next));
                blockFlush(left);
                // 删除parent的key,并向上更新
                removeInNonLeaf(parent, ppos + 1);
            }
        }

    } else {
        // 简单删除一个成员
        simpleRemoveInLeaf(node, pos);
        blockFlush(node);
    }
    return S_OK;
}

void BPlusTree::simpleRemoveInLeaf(Node *node, int pos)
{
    // 节点数减1
    node->count--;
    // 若不是最后一位,则需挪动数据
    if (node->count > pos) {
        memmove(
            &key(node)[pos],
            &key(node)[pos + 1],
            (node->count - pos) * sizeof(key_t));
        memmove(
            &data(node)[pos],
            &data(node)[pos + 1],
            (node->count - pos) * sizeof(data_t));
    }
}

int BPlusTree::selectNode(Node *node, Node *left, Node *right, int pos)
{
    // 若该节点是第一个子节点,则返回右节点
    if (pos == -1)
        return RIGHT_NODE;
    else if (pos == node->count - 1) // 若该节点是最后一个子节点,则返回左节点
        return LEFT_NODE;
    else {
        // 若该节点是中间节点,则返回拥有更多子节点的node
        return left->count >= right->count ? LEFT_NODE : RIGHT_NODE;
    }
}

// 从left移动最后一位到node
void BPlusTree::shiftLeafFromLeft(
    Node *node,
    Node *left,
    Node *parent,
    int ppos,
    int pos)
{
    if (pos != 0) {
        memmove(&key(node)[1], &key(node)[0], pos * sizeof(key_t));
        memmove(&data(node)[1], &data(node)[0], pos * sizeof(data_t));
    }

    key(node)[0] = key(left)[left->count - 1];
    data(node)[0] = data(left)[left->count - 1];
    left->count--;

    // 更新父节点
    key(parent)[ppos] = key(node)[0];
}

// node合并到left
void BPlusTree::mergeLeafIntoLeft(Node *node, Node *left, int pos)
{
    memmove(&key(left)[left->count], &key(node)[0], pos * sizeof(key_t));
    memmove(&data(left)[left->count], &data(node)[0], pos * sizeof(data_t));

    left->count += pos;
    // 剩余从node到left的个数
    int rest = node->count - pos - 1;

    memmove(&key(left)[left->count], &key(node)[pos + 1], rest * sizeof(key_t));
    memmove(
        &data(left)[left->count], &data(node)[pos + 1], rest * sizeof(data_t));

    left->count += rest;
}

// 从right转移一位到node
void BPlusTree::shiftLeafFromRight(
    Node *node,
    Node *right,
    Node *parent,
    int ppos,
    int pos)
{
    key(node)[node->count] = key(right)[0];
    data(node)[node->count] = data(right)[0];
    node->count++;

    right->count--;
    memmove(&key(right)[0], &key(right)[1], right->count * sizeof(key_t));
    memmove(&data(right)[0], &data(right)[1], right->count * sizeof(data_t));

    // 更新parent
    key(parent)[ppos] = key(right)[0];
    if (pos == 0 && ppos > 0) {
        assert(key(parent)[ppos - 1] < key(node)[0]);
        key(parent)[ppos - 1] = key(node)[0];
    }
}

// right合并到node
void BPlusTree::mergeLeafWithRight(Node *node, Node *right)
{
    memmove(
        &key(node)[node->count], &key(right)[0], right->count * sizeof(key_t));
    memmove(
        &data(node)[node->count],
        &data(right)[0],
        right->count * sizeof(data_t));

    node->count += right->count;
}

// 删除非叶子节点
void BPlusTree::removeInNonLeaf(Node *node, int pos)
{
    // node为root节点
    if (traceNode_.empty()) {
        // 若node只有一个key
        if (node->count == 1) {
            assert(pos == 0);
            root_ = *subNode(node, 0);
            removeNode(node, NULL, NULL);
        } else { // 删除一个key
            simpleRemoveInNonLeaf(node, pos);
            blockFlush(node);
        } // NOTE:这里是<,不是<=;
        // 非叶子节点中的key可以比叶子节点中的key还少一个,以下同理
    } else if (node->count < (DEGREE + 1) / 2) {
        // 记录node的父节点和左右节点
        Node *parent = fetchBlock(traceNode_.back());
        Node *left, *right;
        traceNode_.pop_back();

        // NOTE:ppos可能为-1
        int ppos = searchInNode(parent, key(node)[pos]);
        if (ppos >= 0) {
            left = fetchBlock(*subNode(parent, ppos));
            right = fetchBlock(*subNode(parent, ppos + 2));
        } else {
            // node在parent中的key
            int tppos = -ppos - 1;
            if (tppos <= 0) {
                left = NULL;
                right = fetchBlock(*subNode(parent, tppos + 1));
            } else if (tppos >= parent->count) {
                right = NULL;
                left = fetchBlock(*subNode(parent, tppos - 1));

            } else {
                left = fetchBlock(*subNode(parent, tppos - 1));
                right = fetchBlock(*subNode(parent, tppos + 1));
            }
            ppos = -ppos - 2;
        }

        // 选择左右节点
        if (selectNode(parent, left, right, ppos) == LEFT_NODE) {
            // left有足够多的数据,则分一个给node
            if (left->count >= (DEGREE + 1) / 2) {
                // 非叶子节点向左转移一位
                shiftNonLeafFromLeft(node, left, parent, ppos, pos);

                // 把修改刷回磁盘
                blockFlush(parent);
                blockFlush(node);
                blockFlush(left);
                blockFlush(right);
            } else { // left中的key不够
                // node合并到left
                mergeNonLeafIntoLeft(node, left, parent, ppos, pos);

                // 删除node节点
                removeNode(node, NULL, NULL);
                // 把左右节点刷回磁盘
                blockFlush(left);
                blockFlush(right);

                // 向上更新父节点
                removeInNonLeaf(parent, ppos);
            }
        } else { // 选择right节点
            // 先删除node节点中的k
            simpleRemoveInNonLeaf(node, pos);

            // 若right节点右足够多的数据,向左移动一位
            if (right->count >= (DEGREE + 1) / 2) {
                shiftNonLeafFromRight(node, right, parent, ppos + 1, pos);

                // 把修改刷回磁盘
                blockFlush(parent);
                blockFlush(node);
                blockFlush(left);
                blockFlush(right);
            } else { // right中的key不够
                // right合并到right
                mergeNonLeafWithRight(node, right, parent, ppos + 1, pos);

                // 删除right节点
                removeNode(right, NULL, NULL);
                // 把左右节点刷回磁盘
                blockFlush(node);
                blockFlush(left);

                // 向上更新父节点
                removeInNonLeaf(parent, ppos + 1);
            }
        }
    } else {
        // 在node中简单删除pos上的k
        simpleRemoveInNonLeaf(node, pos);
        blockFlush(node);
    }
}

// 在node中删除第pos个数据
void BPlusTree::simpleRemoveInNonLeaf(Node *node, int pos)
{
    int rest = node->count - pos - 1;
    // 当pos == node->count-1时,无需移动数据
    if (rest > 0) {
        memmove(&key(node)[pos], &key(node)[pos + 1], rest * sizeof(key_t));

        // 移动subNode时,需注意lastOffset
        if (node->count == DEGREE) // 使用了lastOffset
        {
            memmove(
                subNode(node, pos + 1),
                subNode(node, pos + 2),
                (rest - 1) * sizeof(off_t));

            *subNode(node, node->count - 1) = *subNode(node, node->count);
        } else { // 未使用lastOffset
            memmove(
                subNode(node, pos + 1),
                subNode(node, pos + 2),
                rest * sizeof(off_t));
        }
    }
    node->count--;
}

// 非叶子节点 left->parent parent->node
void BPlusTree::shiftNonLeafFromLeft(
    Node *node,
    Node *left,
    Node *parent,
    int ppos,
    int pos)
{
    // node->count < (DEGREE + 1) / 2; 未使用lastOffset
    if (pos != 0) {
        memmove(&key(node)[1], &key(node)[0], pos * sizeof(key_t));
        memmove(subNode(node, 1), subNode(node, 0), (pos + 1) * sizeof(off_t));
    }

    key(node)[0] = key(parent)[ppos];
    key(parent)[ppos] = key(left)[left->count - 1];

    // 更新subNode
    *subNode(node, 0) = *subNode(left, left->count);

    left->count--;
}

// node合并到left parent->left node->left
void BPlusTree::mergeNonLeafIntoLeft(
    Node *node,
    Node *left,
    Node *parent,
    int ppos,
    int pos)
{
    // parent->left
    key(left)[left->count] = key(parent)[ppos];
    left->count++;

    memmove(&key(left)[left->count], &key(node)[0], pos * sizeof(key_t));
    memmove(
        subNode(left, left->count),
        subNode(node, 0),
        (pos + 1) * sizeof(off_t));
    left->count += pos;

    // node中除去删除的key,剩余需要转移到left中的key的数量
    int rest = node->count - pos - 1;
    if (rest > 0) {
        memmove(
            &key(left)[left->count], &key(node)[pos + 1], rest * sizeof(key_t));
        // 不会涉及lastOffset
        memmove(
            subNode(left, left->count + 1),
            subNode(node, pos + 2),
            rest * sizeof(off_t));

        left->count += rest;
    }
}

// 非叶子节点 向左移动一位 parent->node right->parent
void BPlusTree::shiftNonLeafFromRight(
    Node *node,
    Node *right,
    Node *parent,
    int ppos,
    int pos)
{
    key(node)[node->count] = key(parent)[ppos];
    key(parent)[ppos] = key(right)[0];

    *subNode(node, node->count + 1) = *subNode(right, 0);
    node->count++;
    right->count--;

    memmove(&key(right)[0], &key(right)[1], right->count * sizeof(key_t));
    memmove(
        subNode(right, 0),
        subNode(right, 1),
        (right->count + 1) * sizeof(off_t));
}

// right合并到node parent->node right->node
void BPlusTree::mergeNonLeafWithRight(
    Node *node,
    Node *right,
    Node *parent,
    int ppos,
    int pos)
{
    key(node)[node->count] = key(parent)[ppos];
    node->count++;

    memmove(
        &key(node)[node->count], &key(right)[0], right->count * sizeof(key_t));
    memmove(
        subNode(node, node->count),
        subNode(right, 0),
        (right->count + 1) * sizeof(off_t));

    node->count += right->count;
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

// 打印所有叶子节点
void BPlusTree::showLeaves()
{
    Node *node = locateNode(root_);

    // 获取最左侧节点
    while (!isLeaf(node)) {
        node = locateNode(*subNode(node, 0));
    }

    // 输出每个节点的key
    assert(node->prev == INVALID_OFFSET);

    int line = 1;
    while (node != NULL) {
        printf("Line %d: ", line++);
        for (int i = 0; i < node->count; i++)
            printf("%ld ", data(node)[i]);
        printf("\n");

        node = locateNode(node->next);
    }
}