##
# @file CMakeLists.txt
# @brief
#  根文件的CMakeLists.txt 
# 
# @author Liu GuangRui
# @email 675040625@qq.com
#

include_directories(${CMAKE_SOURCE_DIR}/include)

set(TEST main.cc)

add_executable(test ${TEST})