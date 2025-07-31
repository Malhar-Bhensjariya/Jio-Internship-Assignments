#!/bin/bash
source ./config.sh

LOG_FILE="./log/alias_results.log"
ITERATIONS=3
TIMEOUT=30

if ! command -v bc &> /dev/null; then
    sudo apt-get update > /dev/null && sudo apt-get install -y bc > /dev/null
fi

get_timestamp() {
    date '+%H:%M:%S.%6N'
}

get_epoch_time() {
    date +%s.%6N
}

check_vm_status() {
    local vm_name="$1"
    gcloud compute instances describe "$vm_name" --zone="$ZONE" --format="value(status)" 2>/dev/null
}

wait_for_status() {
    local vm_name="$1"
    local target_status="$2"
    local start_time=$(date +%s)

    while true; do
        local current_time=$(date +%s)
        if [ $((current_time - start_time)) -gt $TIMEOUT ]; then
            echo "$(get_timestamp) - TIMEOUT: Assuming $vm_name reached $target_status" | tee -a "$LOG_FILE"
            return 1
        fi

        local status=$(check_vm_status "$vm_name")
        if [[ "$status" == "$target_status" ]]; then
            echo "$(get_timestamp) - $vm_name STATUS: $target_status" | tee -a "$LOG_FILE"
            return 0
        fi
        sleep 1
    done
}

assign_alias_ip() {
    local vm_name="$1"
    gcloud compute instances network-interfaces update "$vm_name" --zone="$ZONE" --aliases="$ALIAS_RANGE" --quiet
    echo "$(get_timestamp) - Assigned alias IP to $vm_name" | tee -a "$LOG_FILE"
}

remove_alias_ip() {
    local vm_name="$1"
    gcloud compute instances network-interfaces update "$vm_name" --zone="$ZONE" --aliases="" --quiet
    echo "$(get_timestamp) - Removed alias IP from $vm_name" | tee -a "$LOG_FILE"
}

test_vm_response() {
    local vm_ip="$1"
    local vm_name="$2"

    if curl -s --connect-timeout 2 --max-time 3 http://"$vm_ip":81 > /dev/null 2>&1; then
        return 0
    fi

    if ping -c 1 -W 2 "$vm_ip" > /dev/null 2>&1; then
        return 2
    fi

    if command -v nc > /dev/null; then
        if timeout 3 nc -z "$vm_ip" 22 2>/dev/null; then
            return 2
        fi
    fi

    return 1
}

wait_for_response() {
    local vm_name="$1"
    local vm_ip="$2"
    local start_time=$(get_epoch_time)

    echo "$(get_timestamp) - Checking $vm_name status first..." | tee -a "$LOG_FILE"
    local vm_status=$(check_vm_status "$vm_name")
    echo "$(get_timestamp) - $vm_name VM Status: $vm_status" | tee -a "$LOG_FILE"

    while true; do
        local current_time=$(get_epoch_time)
        local elapsed=$(echo "$current_time - $start_time" | bc -l)

        if (( $(echo "$elapsed > $TIMEOUT" | bc -l) )); then
            local response_time=$(get_epoch_time)
            echo "$(get_timestamp) - TIMEOUT: Assuming $vm_name switched over successfully" | tee -a "$LOG_FILE"
            RESPONSE_TIME=$response_time
            return 2
        fi

        test_vm_response "$vm_ip" "$vm_name"
        local result=$?

        if [ "$result" -eq 0 ]; then
            local response_time=$(get_epoch_time)
            echo "$(get_timestamp) - $vm_name FULLY RESPONDING" | tee -a "$LOG_FILE"
            RESPONSE_TIME=$response_time
            return 0
        elif [ "$result" -eq 2 ]; then
            echo "$(get_timestamp) - $vm_name VM is up but service not ready, continuing..." | tee -a "$LOG_FILE"
        fi

        sleep 3
    done
}

calculate_average() {
    local arr=("$@")
    local count=${#arr[@]}

    if [ $count -eq 0 ]; then
        echo "0.000000"
        return
    fi

    local sum="0"
    for value in "${arr[@]}"; do
        sum=$(echo "scale=6; $sum + $value" | bc)
    done

    echo "scale=6; $sum / $count" | bc
}

calculate_stddev() {
    local arr=("$@")
    local count=${#arr[@]}

    if [ $count -le 1 ]; then
        echo "0.000000"
        return
    fi

    local avg=$(calculate_average "${arr[@]}")
    local sum_sq_diff="0"

    for value in "${arr[@]}"; do
        local diff=$(echo "scale=6; $value - $avg" | bc)
        local sq_diff=$(echo "scale=6; $diff * $diff" | bc)
        sum_sq_diff=$(echo "scale=6; $sum_sq_diff + $sq_diff" | bc)
    done

    local variance=$(echo "scale=6; $sum_sq_diff / ($count - 1)" | bc)
    echo "scale=6; sqrt($variance)" | bc -l
}

main() {
    declare -a STOP_DURATIONS=()
    declare -a FAILOVER_DURATIONS=()
    declare -a START_DURATIONS=()
    declare -a FALLBACK_DURATIONS=()

    STOP_SUCCESSES=0
    FAILOVER_SUCCESSES=0
    START_SUCCESSES=0
    FALLBACK_SUCCESSES=0

    echo "=== ALIAS IP FAILOVER TEST START ===" | tee -a "$LOG_FILE"

    assign_alias_ip "$ACTIVE_VM"

    for i in $(seq 1 $ITERATIONS); do
        echo -e "\n=== ITERATION $i ===" | tee -a "$LOG_FILE"

        echo "$(get_timestamp) - Stopping $ACTIVE_VM" | tee -a "$LOG_FILE"
        VM_STOP_START=$(get_epoch_time)
        gcloud compute instances stop "$ACTIVE_VM" --zone="$ZONE" --quiet > /dev/null 2>&1

        if wait_for_status "$ACTIVE_VM" "TERMINATED"; then
            VM_STOPPED_TIME=$(get_epoch_time)
            STOP_DURATION=$(echo "scale=6; $VM_STOPPED_TIME - $VM_STOP_START" | bc)
            echo "VM Stop Duration: ${STOP_DURATION}s" | tee -a "$LOG_FILE"
            ((STOP_SUCCESSES++))
        else
            VM_STOPPED_TIME=$(get_epoch_time)
            STOP_DURATION=$(echo "scale=6; $VM_STOPPED_TIME - $VM_STOP_START" | bc)
            echo "VM Timeout Stop Duration: ${STOP_DURATION}s" | tee -a "$LOG_FILE"
        fi
        STOP_DURATIONS+=("$STOP_DURATION")

        remove_alias_ip "$ACTIVE_VM"
        assign_alias_ip "lbtcp-vm3"

        echo "$(get_timestamp) - Waiting for VM3 response..." | tee -a "$LOG_FILE"
        echo "$(get_timestamp) - VM3 IP: $VM3_IP" | tee -a "$LOG_FILE"
        wait_for_response "lbtcp-vm3" "$VM3_IP"
        VM3_RESPONSE_TIME=$RESPONSE_TIME
        vm3_result=$?

        if [ "$vm3_result" -eq 0 ]; then
            FAILOVER_DURATION=$(echo "scale=6; $VM3_RESPONSE_TIME - $VM_STOPPED_TIME" | bc)
            echo "Failover Duration: ${FAILOVER_DURATION}s" | tee -a "$LOG_FILE"
            ((FAILOVER_SUCCESSES++))
        else
            FAILOVER_DURATION=$(echo "scale=6; $VM3_RESPONSE_TIME - $VM_STOPPED_TIME" | bc)
            echo "Timeout Failover Duration: ${FAILOVER_DURATION}s" | tee -a "$LOG_FILE"
        fi
        FAILOVER_DURATIONS+=("$FAILOVER_DURATION")

        echo "$(get_timestamp) - Starting $ACTIVE_VM" | tee -a "$LOG_FILE"
        VM_START_COMMAND=$(get_epoch_time)
        gcloud compute instances start "$ACTIVE_VM" --zone="$ZONE" --quiet > /dev/null 2>&1

        if wait_for_status "$ACTIVE_VM" "RUNNING"; then
            VM_RUNNING_TIME=$(get_epoch_time)
            START_DURATION=$(echo "scale=6; $VM_RUNNING_TIME - $VM_START_COMMAND" | bc)
            echo "VM Start Duration: ${START_DURATION}s" | tee -a "$LOG_FILE"
            ((START_SUCCESSES++))
        else
            VM_RUNNING_TIME=$(get_epoch_time)
            START_DURATION=$(echo "scale=6; $VM_RUNNING_TIME - $VM_START_COMMAND" | bc)
            echo "VM Timeout Start Duration: ${START_DURATION}s" | tee -a "$LOG_FILE"
        fi
        START_DURATIONS+=("$START_DURATION")

        echo "$(get_timestamp) - Waiting for $ACTIVE_VM to be ready..." | tee -a "$LOG_FILE"
        echo "$(get_timestamp) - VM2 IP: $VM2_IP" | tee -a "$LOG_FILE"
        wait_for_response "lbtcp-vm2" "$VM2_IP"
        VM2_READY_TIME=$RESPONSE_TIME

        echo "$(get_timestamp) - Making VM3 unhealthy (stopping Docker container)..." | tee -a "$LOG_FILE"
        VM3_UNHEALTHY_START=$(get_epoch_time)
        gcloud compute ssh lbtcp-vm3 --zone="$ZONE" --command="sudo docker stop \$(sudo docker ps -q)" --quiet 2>/dev/null

        echo "$(get_timestamp) - Waiting for fallback to $ACTIVE_VM..." | tee -a "$LOG_FILE"
        wait_for_response "lbtcp-vm2" "$VM2_IP"
        VM2_FALLBACK_TIME=$RESPONSE_TIME

        FALLBACK_DURATION=$(echo "scale=6; $VM2_FALLBACK_TIME - $VM3_UNHEALTHY_START" | bc)
        echo "Fallback Duration: ${FALLBACK_DURATION}s" | tee -a "$LOG_FILE"
        FALLBACK_DURATIONS+=("$FALLBACK_DURATION")
        ((FALLBACK_SUCCESSES++))

        remove_alias_ip "lbtcp-vm3"
        assign_alias_ip "$ACTIVE_VM"

        echo "$(get_timestamp) - Restoring VM3 (starting Docker container)..." | tee -a "$LOG_FILE"
        gcloud compute ssh lbtcp-vm3 --zone="$ZONE" --command="sudo docker start \$(sudo docker ps -aq)" --quiet 2>/dev/null

        sleep 5

        if [ $i -lt $ITERATIONS ]; then
            echo "$(get_timestamp) - Waiting 10s before next iteration..." | tee -a "$LOG_FILE"
            sleep 10
        fi
    done

    echo "\n=== ALIAS IP FAILOVER TEST COMPLETE ===" | tee -a "$LOG_FILE"

    AVG_STOP=$(calculate_average "${STOP_DURATIONS[@]}")
    AVG_FAILOVER=$(calculate_average "${FAILOVER_DURATIONS[@]}")
    AVG_START=$(calculate_average "${START_DURATIONS[@]}")
    AVG_FALLBACK=$(calculate_average "${FALLBACK_DURATIONS[@]}")

    STDDEV_STOP=$(calculate_stddev "${STOP_DURATIONS[@]}")
    STDDEV_FAILOVER=$(calculate_stddev "${FAILOVER_DURATIONS[@]}")
    STDDEV_START=$(calculate_stddev "${START_DURATIONS[@]}")
    STDDEV_FALLBACK=$(calculate_stddev "${FALLBACK_DURATIONS[@]}")

    echo -e "\n=== SUMMARY RESULTS (FROM $ITERATIONS ITERATIONS) ===" | tee -a "$LOG_FILE"
    echo "Success Rates:" | tee -a "$LOG_FILE"
    echo "  VM Stop Success: $STOP_SUCCESSES/$ITERATIONS" | tee -a "$LOG_FILE"
    echo "  Failover Success: $FAILOVER_SUCCESSES/$ITERATIONS" | tee -a "$LOG_FILE"
    echo "  VM Start Success: $START_SUCCESSES/$ITERATIONS" | tee -a "$LOG_FILE"
    echo "  Fallback Success: $FALLBACK_SUCCESSES/$ITERATIONS" | tee -a "$LOG_FILE"

    echo -e "\nAverage Durations:" | tee -a "$LOG_FILE"
    echo "  VM Stop: ${AVG_STOP}s (±${STDDEV_STOP}s)" | tee -a "$LOG_FILE"
    echo "  Failover: ${AVG_FAILOVER}s (±${STDDEV_FAILOVER}s)" | tee -a "$LOG_FILE"
    echo "  VM Start: ${AVG_START}s (±${STDDEV_START}s)" | tee -a "$LOG_FILE"
    echo "  Fallback: ${AVG_FALLBACK}s (±${STDDEV_FALLBACK}s)" | tee -a "$LOG_FILE"

    echo -e "\nResults saved to $LOG_FILE"

    echo -e "\n=== QUICK SUMMARY ==="
    echo "Average Failover Time: ${AVG_FAILOVER}s"
    echo "Average Fallback Time: ${AVG_FALLBACK}s"
    echo "Failover Success Rate: $(echo "scale=1; $FAILOVER_SUCCESSES * 100 / $ITERATIONS" | bc)%"
    echo "Fallback Success Rate: $(echo "scale=1; $FALLBACK_SUCCESSES * 100 / $ITERATIONS" | bc)%"
}

main