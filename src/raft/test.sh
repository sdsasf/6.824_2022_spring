#!/bin/bash

for i in {1..1000}; do
    echo "ieration $i"
    # 运行go test并捕获退出状态码
    go test --run 2B
    EXIT_STATUS=$?
    
    # 检查退出状态码，如果不为0，则表示测试失败
    if [ $EXIT_STATUS -ne 0 ]; then
        echo "Tests failed"
        exit 1
    fi
    
    echo "Tests passed, running again..."
done
