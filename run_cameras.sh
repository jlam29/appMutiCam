#!/bin/bash

LOG="/tmp/camera_startup.log"
echo "=== Démarrage $(date) ===" >> "$LOG"

# Attendre que les devices soient disponibles
for i in {1..10}; do
    if [ -e /dev/video0 ] && [ -e /dev/video2 ]; then
        echo "✓ Devices disponibles" >> "$LOG"
        break
    fi
    echo "Attente des devices... ($i/10)" >> "$LOG"
    sleep 1
done

# Configuration MJPEG
echo "Configuration MJPEG..." >> "$LOG"
v4l2-ctl --device=/dev/video0 --set-fmt-video=width=640,height=480,pixelformat=MJPG >> "$LOG" 2>&1
v4l2-ctl --device=/dev/video2 --set-fmt-video=width=640,height=480,pixelformat=MJPG >> "$LOG" 2>&1

# Vérification
v4l2-ctl --device=/dev/video0 --get-fmt-video >> "$LOG" 2>&1
v4l2-ctl --device=/dev/video2 --get-fmt-video >> "$LOG" 2>&1

sleep 2

# Lancer l'application
echo "Lancement de l'application..." >> "$LOG"
cd /home/nemo/Documents/CameraVisual/cameras
export HOME=/home/nemo
exec python3 appMultiCam.py