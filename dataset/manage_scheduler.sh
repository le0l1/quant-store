#!/bin/bash
# 调度器管理脚本

# 设置时区为东八区（北京时间）
export TZ='Asia/Shanghai'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCHEDULER_LOG="$SCRIPT_DIR/scheduler.log"
PID_FILE="$SCRIPT_DIR/scheduler.pid"

case "$1" in
    start)
        echo "启动调度器..."
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p $PID > /dev/null 2>&1; then
                echo "调度器已在运行 (PID: $PID)"
                exit 1
            else
                rm -f "$PID_FILE"
            fi
        fi
        
        nohup env TZ='Asia/Shanghai' uv run python scheduler.py > "$SCHEDULER_LOG" 2>&1 &
        echo $! > "$PID_FILE"
        echo "调度器已启动 (PID: $!)"
        echo "日志文件: $SCHEDULER_LOG"
        ;;
    
    stop)
        echo "停止调度器..."
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p $PID > /dev/null 2>&1; then
                kill $PID
                rm -f "$PID_FILE"
                echo "调度器已停止"
            else
                echo "调度器未运行"
                rm -f "$PID_FILE"
            fi
        else
            echo "调度器未运行"
        fi
        ;;
    
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    
    status)
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if ps -p $PID > /dev/null 2>&1; then
                echo "调度器正在运行 (PID: $PID)"
                echo "日志文件: $SCHEDULER_LOG"
            else
                echo "调度器未运行 (PID文件存在但进程不存在)"
                rm -f "$PID_FILE"
            fi
        else
            echo "调度器未运行"
        fi
        ;;
    
    logs)
        if [ -f "$SCHEDULER_LOG" ]; then
            tail -f "$SCHEDULER_LOG"
        else
            echo "日志文件不存在"
        fi
        ;;
    
    *)
        echo "用法: $0 {start|stop|restart|status|logs}"
        echo ""
        echo "命令说明:"
        echo "  start   - 启动调度器"
        echo "  stop    - 停止调度器"
        echo "  restart - 重启调度器"
        echo "  status  - 查看调度器状态"
        echo "  logs    - 查看实时日志"
        exit 1
        ;;
esac 