# /bin/bash

# 发布本地依赖脚本
ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-astellas/target/"
scp -r pharbers-max-astellas/target/pharbers-max-astellas-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-astellas/target/pharbers-max-astellas-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-nhwa/target/"
scp -r pharbers-max-nhwa/target/pharbers-max-nhwa-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-nhwa/target/pharbers-max-nhwa-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-pfizer/target/"
scp -r pharbers-max-pfizer/target/pharbers-max-pfizer-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-pfizer/target/pharbers-max-pfizer-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-servier/target/"
scp -r pharbers-max-servier/target/pharbers-max-servier-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-servier/target/pharbers-max-servier-1.0.jar

scp -r pharbers-max-main/target/lib spark@max.logic:/home/spark/max_driver/
scp -r pharbers-max-main/target/pharbers-max-main-1.0.jar spark@max.logic:/home/spark/max_driver/

# 启动命令
# nohup java -jar pharbers-max-main-1.0.jar 1>output.log 2>&1 &
