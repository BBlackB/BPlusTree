##
# @file CMakeLists.txt
# @brief
#  根文件的CMakeLists.txt 
# 
# @author Liu GuangRui
# @email 675040625@qq.com
#

include_directories(${CMAKE_SOURCE_DIR}/include)

set(LIB_BPLUSTREE_SRC xxx.cc)

add_library(BPlusTree ${LIB_BPLUSTREE_SRC})

