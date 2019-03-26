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
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <list>

// #define BPTREE_DEGREE 3
#define key_t long
#define S_OK 0
#define S_FALSE -1

#pragma pack(push)
#pragma pack(2)
struct Node
{
    off_t self;
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
    const int ADDR_OFFSET_LENTH = 16;        // 每次读取文件的长度
    const off_t INVALID_OFFSET = 0xDEADBEEF; // 错误的文件偏移量

  private:
    off_t root_;                  // 记录root的偏移量
    off_t blockSize_;             // 块大小
    off_t fileSize_;              // 指向文件末尾,便于创建新的block
    std::list<off_t> freeBlocks_; // 记录空闲块
    const char *fileName_;        // 索引文件
    int fd_;                      // 索引文件的描述符
    int DEGREE;                   // 一个block中的最大节点

  public:
    BPlusTree(const char *fileName, int blockSize);
    ~BPlusTree();

    // 执行命令
    void commandHander();

  private:
    // 显示帮助信息
    void help();
    // 显示树中所有节点
    int dump();

  private:
    // 读取一个偏移量
    off_t offsetLoad(int fd);
    // 存一个偏移量
    int offsetStore(int fd, off_t offset);

  private:
    // 字符串转换为off_t
    off_t pchar_2_off_t(const char *str, size_t size);
    // off_t转换为字符串
    void off_t_2_pchar(off_t offset, char *buf, int len);
};

#endif // __BPLUSTREE_H__