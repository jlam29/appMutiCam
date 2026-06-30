#!/bin/bash
echo "=== Rapport Système Caméras $(date) ===" 

echo "Services:"
systemctl is-active camera-web camera-subscriber camera-watchdog

echo ""
echo "Uptime camera-web:"
systemctl show camera-web -p ActiveEnterTimestamp --value

echo ""
echo "Mémoire utilisée:"
ps aux | grep "python3.*appMultiCam" | grep -v grep | awk '{print "  PID: "$2" - MEM: "$4"%  - CPU: "$3"%"}'

echo ""
echo "Événements dernières 24h:"
sqlite3 /home/nemo/Documents/CameraVisual/camera_logs/camera_activity.db \
  "SELECT COUNT(*) as total FROM camera_events WHERE timestamp >= datetime('now', '-24 hours');"

echo ""
echo "Derniers événements:"
sqlite3 /home/nemo/Documents/CameraVisual/camera_logs/camera_activity.db \
  "SELECT datetime(timestamp, 'localtime'), camera_id, status FROM camera_events ORDER BY timestamp DESC LIMIT 5;"