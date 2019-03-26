/*
 * @file main.c
 * @brief
 * B+Tree
 *
 * @author Liu GuangRui
 * @email 675040625@qq.com
 */
#include "BPlusTree.h"

int main(int argc, char const *argv[])
{
    BPlusTree bpTree("data.index", 128);
    bpTree.commandHander();

    return 0;
}
