# /bin/bash

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-astellas/target/"
scp -r pharbers-max-astellas/target/pharbers-max-astellas-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-astellas/target/pharbers-max-astellas-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-nhwa/target/"
scp -r pharbers-max-nhwa/target/pharbers-max-nhwa-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-nhwa/target/pharbers-max-nhwa-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-pfizer/target/"
scp -r pharbers-max-pfizer/target/pharbers-max-pfizer-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-pfizer/target/pharbers-max-pfizer-1.0.jar

ssh spark@max.logic "mkdir -p /Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-servier/target/"
scp -r pharbers-max-servier/target/pharbers-max-servier-1.0.jar spark@max.logic:/Users/clock/workSpace/Pharbers/pharbers-max-builder/pharbers-max-servier/target/pharbers-max-servier-1.0.jar

