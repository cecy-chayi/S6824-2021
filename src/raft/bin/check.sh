#!/bin/bash

# 创建结果目录
mkdir -p test_results

# 定义测试任务数组，按类别分组
TESTS=(
  "TestInitialElection2A"
  "TestReElection2A"
  "TestManyElections2A"
  "TestBasicAgree2B"
  "TestRPCBytes2B"
  "TestFailAgree2B"
  "TestFailNoAgree2B"
  "TestConcurrentStarts2B"
  "TestRejoin2B"
  "TestBackup2B"
  "TestCount2B"
  "TestPersist12C"
  "TestPersist22C"
  "TestPersist32C"
  "TestFigure82C"
  "TestUnreliableAgree2C"
  "TestFigure8Unreliable2C"
  "TestSnapshotInstallUnCrash2D"
)

# 创建结果文件
RESULT_FILE="test_results/summary_$(date +%Y%m%d_%H%M%S).log"
echo "测试结果汇总 - $(date)" > "$RESULT_FILE"
echo "=================================" >> "$RESULT_FILE"

# 初始化计数器
TOTAL_REAL_TIME=0
TOTAL_USER_TIME=0
TOTAL_SYS_TIME=0
TOTAL_CPU_TIME=0

# 初始化各阶段计数器
PHASE_2A_REAL=0
PHASE_2A_CPU=0
PHASE_2B_REAL=0
PHASE_2B_CPU=0
PHASE_2C_REAL=0
PHASE_2C_CPU=0
PHASE_2D_REAL=0
PHASE_2D_CPU=0

# 循环运行所有测试
for test_name in "${TESTS[@]}"; do
  echo "正在运行测试: $test_name"
  
  # 使用time命令运行测试并记录时间，将输出保存到单独的文件
  test_output="test_results/${test_name}_output.log"
  
  # 使用GNU time的-f选项格式化输出，或者使用bash内置time并解析输出
  { time go test -run "^$test_name$" -race > "$test_output" 2>&1; } 2> time_output.txt
  
  # 获取测试结果
  if tail -1 "$test_output" | grep -q "FAIL"; then
    result="失败"
  else
    result="通过"
  fi
  
  # 解析时间信息（bash内置time的输出格式）
  time_info=$(cat time_output.txt)
  
  # 提取real、user和sys时间
  # 假设输出格式为: real    0m0.123s
  #               user    0m0.456s
  #               sys     0m0.789s
  real_time=$(echo "$time_info" | grep "real" | awk '{print $2}' | sed 's/m/ /;s/s//' | awk '{print $1*60+$2}')
  user_time=$(echo "$time_info" | grep "user" | awk '{print $2}' | sed 's/m/ /;s/s//' | awk '{print $1*60+$2}')
  sys_time=$(echo "$time_info" | grep "sys" | awk '{print $2}' | sed 's/m/ /;s/s//' | awk '{print $1*60+$2}')
  cpu_time=$(echo "$user_time + $sys_time" | bc)
  
  # 累加总时间
  TOTAL_REAL_TIME=$(echo "$TOTAL_REAL_TIME + $real_time" | bc)
  TOTAL_USER_TIME=$(echo "$TOTAL_USER_TIME + $user_time" | bc)
  TOTAL_SYS_TIME=$(echo "$TOTAL_SYS_TIME + $sys_time" | bc)
  TOTAL_CPU_TIME=$(echo "$TOTAL_CPU_TIME + $cpu_time" | bc)
  
  # 按测试阶段累加时间
  if [[ "$test_name" == *"2A"* ]]; then
    PHASE_2A_REAL=$(echo "$PHASE_2A_REAL + $real_time" | bc)
    PHASE_2A_CPU=$(echo "$PHASE_2A_CPU + $cpu_time" | bc)
  elif [[ "$test_name" == *"2B"* ]]; then
    PHASE_2B_REAL=$(echo "$PHASE_2B_REAL + $real_time" | bc)
    PHASE_2B_CPU=$(echo "$PHASE_2B_CPU + $cpu_time" | bc)
  elif [[ "$test_name" == *"2C"* ]]; then
    PHASE_2C_REAL=$(echo "$PHASE_2C_REAL + $real_time" | bc)
    PHASE_2C_CPU=$(echo "$PHASE_2C_CPU + $cpu_time" | bc)
  elif [[ "$test_name" == *"2D"* ]]; then
    PHASE_2D_REAL=$(echo "$PHASE_2D_REAL + $real_time" | bc)
    PHASE_2D_CPU=$(echo "$PHASE_2D_CPU + $cpu_time" | bc)
  fi
  
  # 输出到控制台
  echo "测试 $test_name: $result"
  echo "Real时间: ${real_time}s, CPU时间: ${cpu_time}s (User: ${user_time}s, Sys: ${sys_time}s)"
  echo "---------------------------------"
  
  # 保存到结果文件
  echo "测试: $test_name" >> "$RESULT_FILE"
  echo "结果: $result" >> "$RESULT_FILE"
  echo "Real时间: ${real_time}s" >> "$RESULT_FILE"
  echo "User时间: ${user_time}s" >> "$RESULT_FILE"
  echo "Sys时间: ${sys_time}s" >> "$RESULT_FILE"
  echo "总CPU时间: ${cpu_time}s" >> "$RESULT_FILE"
  echo "---------------------------------" >> "$RESULT_FILE"
  
  # 短暂暂停以避免系统资源过度占用
  sleep 1
done

# 添加汇总统计到结果文件
echo "\n=================================" >> "$RESULT_FILE"
echo "测试时间汇总统计" >> "$RESULT_FILE"
echo "=================================" >> "$RESULT_FILE"

# 按阶段汇总
echo "2A 阶段汇总:" >> "$RESULT_FILE"
echo "  总Real时间: ${PHASE_2A_REAL}s" >> "$RESULT_FILE"
echo "  总CPU时间: ${PHASE_2A_CPU}s" >> "$RESULT_FILE"
echo "2B 阶段汇总:" >> "$RESULT_FILE"
echo "  总Real时间: ${PHASE_2B_REAL}s" >> "$RESULT_FILE"
echo "  总CPU时间: ${PHASE_2B_CPU}s" >> "$RESULT_FILE"
echo "2C 阶段汇总:" >> "$RESULT_FILE"
echo "  总Real时间: ${PHASE_2C_REAL}s" >> "$RESULT_FILE"
echo "  总CPU时间: ${PHASE_2C_CPU}s" >> "$RESULT_FILE"
echo "2D 阶段汇总:" >> "$RESULT_FILE"
echo "  总Real时间: ${PHASE_2D_REAL}s" >> "$RESULT_FILE"
echo "  总CPU时间: ${PHASE_2D_CPU}s" >> "$RESULT_FILE"

echo "\n总体汇总:" >> "$RESULT_FILE"
echo "  总Real时间: ${TOTAL_REAL_TIME}s" >> "$RESULT_FILE"
echo "  总User时间: ${TOTAL_USER_TIME}s" >> "$RESULT_FILE"
echo "  总Sys时间: ${TOTAL_SYS_TIME}s" >> "$RESULT_FILE"
echo "  总CPU时间: ${TOTAL_CPU_TIME}s" >> "$RESULT_FILE"

# 计算CPU使用率
echo "  CPU使用率: $(echo "scale=2; $TOTAL_CPU_TIME/$TOTAL_REAL_TIME*100" | bc)%" >> "$RESULT_FILE"

# 输出到控制台
echo "\n================================="
echo "测试时间汇总统计"
echo "================================="
echo "2A 阶段汇总: Real ${PHASE_2A_REAL}s, CPU ${PHASE_2A_CPU}s"
echo "2B 阶段汇总: Real ${PHASE_2B_REAL}s, CPU ${PHASE_2B_CPU}s"
echo "2C 阶段汇总: Real ${PHASE_2C_REAL}s, CPU ${PHASE_2C_CPU}s"
echo "2D 阶段汇总: Real ${PHASE_2D_REAL}s, CPU ${PHASE_2D_CPU}s"
echo "\n总体汇总:"
echo "总Real时间: ${TOTAL_REAL_TIME}s"
echo "总CPU时间: ${TOTAL_CPU_TIME}s (User: ${TOTAL_USER_TIME}s, Sys: ${TOTAL_SYS_TIME}s)"
echo "CPU使用率: $(echo "scale=2; $TOTAL_CPU_TIME/$TOTAL_REAL_TIME*100" | bc)%"
echo "\n所有测试完成！"
echo "详细结果请查看: $RESULT_FILE"