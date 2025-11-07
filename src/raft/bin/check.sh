#!/bin/bash

while true; do
    # 运行测试并将输出保存到文件
    go test -run 2B -race > txt.log 2>&1
    
    # 检查输出中是否包含 FAIL
    if grep -q "FAIL" txt.log; then
        echo "测试失败，停止执行"
        tail -1 txt.log  # 显示最后一行
        break  # 退出循环
    else
        echo "测试通过，继续运行..."
        # sleep 1  # 可选：添加短暂延迟避免 CPU 占用过高
    fi
    
    # 检查是否按下 Ctrl+C 退出
    if [[ $? -eq 130 ]]; then
        echo "用户中断"
        break
    fi
done