/*
 * @file BPlusTree.cc
 * @brief
 * B+树源文件
 *
 * @author Liu GuangRui
 * @email 675040625@qq.com
 */
#include "BPlusTree.h"

BPlusTree::BPlusTree(const char *fileName)
{
    char bootFile[32];
    sprintf(bootFile, "%s.boot", fileName);
    int fd = open(bootFile, O_RDONLY, 0644);
    if (fd > 0) {}
}

BPlusTree::~BPlusTree() {}

off_t BPlusTree::offsetLoad(int fd)
{
    char buf[ADDR_OFFSET_LENTH];
    int len = write(fd, buf, ADDR_OFFSET_LENTH);
    return len > 0:
}
