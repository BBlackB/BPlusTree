##
# @file CMakeLists.txt
# @brief
#  根文件的CMakeLists.txt 
# 
# @author Liu GuangRui
# @email 675040625@qq.com
#

cmake_minimum_required(VERSION 2.8)

project(BPlusTree)

message(STATUS "### Begin to configure project BPlusTree ###")

# 检测cmake运行的操作系统/CPU/编译器
message(STATUS "Generator: ${CMAKE_GENERATOR}")
message(STATUS "Building OS: ${CMAKE_SYSTEM_NAME}")
message(STATUS "Building Processor: ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "Compiler: ${CMAKE_C_COMPILER_ID}")

# C++11标准
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
 
# 设置编译结果输出路径
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG "${PROJECT_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE "${PROJECT_BINARY_DIR}/bin")


# 编译模式，Release/Debug，-DCMAKE_BUILD_TYPE=Release|Debug
message(STATUS "Building mode: ${CMAKE_BUILD_TYPE}")

# 设置编译参数
set(CMAKE_C_FLAGS                     "-Wall -Werror -Wno-attributes")
set(CMAKE_C_FLAGS_DEBUG               "-g -DDEBUG") #-fstack-protector-all
set(CMAKE_C_FLAGS_RELEASE             "-O3")
set(CMAKE_C_FLAGS_MINSIZEREL          "-Os")
set(CMAKE_C_RELWITHDEBINFO_RELEASE    "-O2")
set(CMAKE_CXX_FLAGS                   "-Wall -Werror -Wno-attributes -ftemplate-backtrace-limit=0 -fno-exceptions -std=\"c++11\"")
set(CMAKE_CXX_FLAGS_DEBUG             "-g -DDEBUG")
set(CMAKE_CXX_FLAGS_RELEASE           "-O3 -fno-strict-aliasing")
set(CMAKE_CXX_FLAGS_MINSIZEREL        "-Os")
set(CMAKE_CXX_RELWITHDEBINFO_RELEASE  "-O2")

add_subdirectory(src)
add_subdirectory(tests)

message(STATUS "### Done ###")