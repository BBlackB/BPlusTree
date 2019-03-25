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

#define BPTREE_DEGREE 3

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

class BPlusTree
{
    // 一些常量
  private:
    const int ADDR_OFFSET_LENTH = 16; // 每次读取文件的长度

  private:
    off_t root_;                  // 记录root的偏移量
    int fd_;                      // 索引文件的描述符
    std::list<off_t> freeBlocks_; // 记录空闲块
    int fileSize_;                // 指向文件末尾,便于创建新的block

  public:
    BPlusTree(const char *fileName);
    ~BPlusTree();

    // 显示树中所有节点
    int dump();

  private:
    off_t offsetLoad(int fd);  // 读取一个偏移量
    off_t offsetStore(int fd); // 存一个偏移量
};

#endif // __BPLUSTREE_H__