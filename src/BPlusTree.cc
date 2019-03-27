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
        case 'r':
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

int BPlusTree::insert(key_t key, off_t value) {}

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
    insert(n, n);
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