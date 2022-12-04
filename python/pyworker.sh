#!/bin/bash
function log_failed_msg() {
    echo "$@" "[ FAILED ]"
}

function log_success_msg() {
    echo "$@" "[ OK ]"
}

function Pyworker_usage() {
    echo "Usage : $0 {start|stop|restart}"
    echo " <-config Pyworker config> <-ip Pyworker ip> <-port Pyworker port> <-pidfile Pyworker pidfile> <-log Pyworker log>"
    exit 1
}


function start() {
    if [ -f $PID_FILE ]; then
        PID="$(cat $PID_FILE)"
        if kill -0 "$PID" &>/dev/null; then
            # Pyworker is already running
            log_success_msg "Pyworker is already running"
            return 0
        fi
    else
        touch $PID_FILE &>/dev/null
    fi

    # start Pyworker
    echo "Starting Pyworker..."
    nohup python3 -u -m ts-udf.server.handler --config $PYWORKER_CONFIG_FILE --ip $PYWORKER_IP --port $PYWORKER_PORT --pidfile $PID_FILE > $LOG_FILE 2>&1 &
    # sleep to verify Pyworker is up
    sleep 10
    if [ -f $PID_FILE ]; then
        if kill -0 $(cat $PID_FILE) &>/dev/null; then
            log_success_msg "Pyworker was started"
            return 0
        fi
    fi
    log_failed_msg "Pyworker was unable to start"
    exit 1
}


function stop() {
    if [ -f $PID_FILE ]; then
        local PID="$(cat $PID_FILE)"
        if kill -0 $PID &>/dev/null; then
            echo "Stopping Pyworker..."
            kill -s TERM $PID &>/dev/null && rm -f "$PID_FILE" &>/dev/null
            n=0
            while true; do
                # Enter loop to ensure Pyworker is stopped
                kill -0 $PID &>/dev/null
                if [ "$?" != "0" ]; then
                    log_success_msg "Pyworker was stopped"
                    return 0
                fi

                # Pyworker still alive after signal, sleep and wait
                sleep 5
                n=$(expr $n + 1)
                if [ $n -eq 30 ]; then
                    # After 30 seconds, send SIGKILL
                    echo "Timeout exceeded, sending SIGKILL..."
                    kill -s KILL $PID &>/dev/null
                elif [ $? -eq 40 ]; then
                    # After 40 seconds, error out
                    log_failure_msg "could not stop Pyworker"
                    exit 1
                fi
            done
        fi
    fi
    log_success_msg "Pyworker process already stopped"
}

function restart() {
    stop
    start
}

function status() {
    # Check the status of the process.
    if [ -f $PID_FILE ]; then
        PID="$(cat $PID_FILE)"
        if kill -0 $PID &>/dev/null; then
            log_success_msg "Pyworker is running"
            exit 0
        fi
    fi
    log_failure_msg "Pyworker is not running"
    exit 1
}


COMMAND=$1
shift
while [ $# -ne 0 ]; do
    case "$1" in
        -config)
            PYWORKER_CONFIG_FILE=$2
            echo "$PYWORKER_CONFIG_FILE"
            shift
            ;;
        -ip)
            PYWORKER_IP=$2
            echo "$PYWORKER_IP"
            shift
            ;;
        -port)
            PYWORKER_PORT=$2
            echo "$PYWORKER_PORT"
            shift
            ;;
        -pidfile)
            PID_FILE=$2
            echo "$PID_FILE"
            shift
            ;;
        -log)
            LOG_FILE=$2
            echo "$LOG_FILE"
            shift
            ;;
        *)
            echo "ERROR: Free commandline arguments are not allowed"
            Pyworker_usage
            RETVAL=1
            exit 1
            ;;
    esac
    shift
done

case $COMMAND in
    start)
        start
        ;;
    
    stop)
        stop
        ;;
    
    restart)
        restart
        ;;
    
    status)
        status
        ;;
    
    *)
        # For invalid arguments, print the usage message.
        echo "Usage: $0 {start|stop|restart|status}"
        exit 2
        ;;
esac
