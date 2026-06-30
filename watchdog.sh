#!/bin/bash

LOG_FILE="/home/nemo/Documents/CameraVisual/watchdog.log"
DB_PATH="/home/nemo/Documents/CameraVisual/camera_logs/camera_activity.db"
CHECK_INTERVAL=60

log_message() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg" | tee -a "$LOG_FILE"
    
    # Aussi dans la DB
    log_to_db "log" "$1"
}

log_to_db() {
    local event_type="$1"
    local message="$2"
    
    sqlite3 "$DB_PATH" <<EOF
INSERT INTO watchdog_events (event_type, message, metadata)
VALUES ('$event_type', '$message', '{"hostname": "$(hostname)", "user": "$USER"}');
EOF
}

check_service() {
    if ! systemctl is-active --quiet camera-web; then
        log_message "⚠️ Service camera-web arrêté, redémarrage..."
        log_to_db "service_restart" "Service camera-web was down"
        sudo systemctl restart camera-web
        sleep 10
        
        if systemctl is-active --quiet camera-web; then
            log_message "✓ Service camera-web redémarré avec succès"
            log_to_db "service_restart_success" "Service camera-web restarted successfully"
        else
            log_message "✗ Échec du redémarrage de camera-web"
            log_to_db "service_restart_failed" "Failed to restart camera-web"
        fi
        return 1
    fi
    return 0
}

check_web_app() {
    if ! curl -s --max-time 5 http://localhost:5000 > /dev/null 2>&1; then
        log_message "⚠️ Application web ne répond pas, redémarrage..."
        log_to_db "app_not_responding" "Web app timeout after 5s"
        sudo systemctl restart camera-web
        sleep 10
        
        if curl -s --max-time 5 http://localhost:5000 > /dev/null 2>&1; then
            log_message "✓ Application web redémarrée avec succès"
            log_to_db "app_restart_success" "Web app responding again"
        else
            log_message "✗ Application web ne répond toujours pas"
            log_to_db "app_restart_failed" "Web app still not responding"
        fi
        return 1
    fi
    return 0
}

check_memory() {
    # Vérifier l'utilisation mémoire
    local mem_usage=$(ps aux | grep "python3.*appMultiCam" | grep -v grep | awk '{print $4}' | head -n1)
    
    if [ -n "$mem_usage" ]; then
        # Si mémoire > 50%, redémarrer
        if (( $(echo "$mem_usage > 50" | bc -l) )); then
            log_message "⚠️ Utilisation mémoire élevée: ${mem_usage}%, redémarrage..."
            log_to_db "high_memory" "Memory usage at ${mem_usage}%"
            sudo systemctl restart camera-web
            sleep 10
            log_message "✓ Service redémarré suite à forte utilisation mémoire"
            log_to_db "memory_restart_success" "Restarted due to high memory"
            return 1
        fi
    fi
    return 0
}

check_subscriber() {
    if ! systemctl is-active --quiet camera-subscriber; then
        log_message "⚠️ Service camera-subscriber arrêté, redémarrage..."
        log_to_db "subscriber_restart" "Subscriber was down"
        sudo systemctl