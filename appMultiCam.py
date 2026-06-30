from flask import Flask, render_template, Response, jsonify
import cv2
import threading
import time
import sqlite3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import shutil
import os

app = Flask(__name__)

# Configuration
DB_PATH = '/home/nemo/Documents/CameraVisual/camera_logs/camera_activity.db'
GRAFANA_DB_PATH = '/home/nemo/Documents/CameraVisual/grafana_data/camera_activity.db'
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC = 'cameras/status'

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_db()
    
    def init_db(self):
        """Initialise la base de données"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        with self.lock:
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=10)
                cursor = conn.cursor()
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS camera_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        camera_id INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        duration_seconds INTEGER,
                        metadata TEXT
                    )
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_camera_timestamp 
                    ON camera_events(camera_id, timestamp)
                ''')
                
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_timestamp 
                    ON camera_events(timestamp)
                ''')
                
                conn.commit()
            except Exception as e:
                print(f"Erreur init DB: {e}")
            finally:
                if conn:
                    conn.close()
    
    def log_event(self, camera_id, status, event_type='toggle', duration=None, metadata=None):
        """Enregistre un événement caméra avec gestion d'erreur"""
        with self.lock:
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=10)
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO camera_events 
                    (camera_id, status, event_type, duration_seconds, metadata)
                    VALUES (?, ?, ?, ?, ?)
                ''', (camera_id, status, event_type, duration, json.dumps(metadata) if metadata else None))
                
                conn.commit()
                self.sync_to_grafana()
            except sqlite3.OperationalError as e:
                print(f"⚠ DB occupée: {e}")
            except Exception as e:
                print(f"✗ Erreur log DB: {e}")
            finally:
                if conn:
                    conn.close()
    
    def sync_to_grafana(self):
        """Copie la base de données vers l'emplacement Grafana"""
        try:
            os.makedirs(os.path.dirname(GRAFANA_DB_PATH), exist_ok=True)
            shutil.copy2(self.db_path, GRAFANA_DB_PATH)
        except Exception as e:
            pass
    
    def get_camera_stats(self, camera_id, hours=24):
        """Récupère les statistiques d'une caméra"""
        with self.lock:
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=10)
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT status, COUNT(*) as count, 
                           AVG(duration_seconds) as avg_duration
                    FROM camera_events
                    WHERE camera_id = ? 
                    AND timestamp >= datetime('now', '-' || ? || ' hours')
                    GROUP BY status
                ''', (camera_id, hours))
                
                results = cursor.fetchall()
                return results
            except Exception as e:
                print(f"✗ Erreur stats DB: {e}")
                return []
            finally:
                if conn:
                    conn.close()

class MQTTPublisher:
    def __init__(self):
        self.client = mqtt.Client(client_id="camera_web_app", clean_session=True)
        self.connected = False
        self.lock = threading.Lock()
        self.setup_mqtt()
    
    def setup_mqtt(self):
        """Configure le client MQTT"""
        try:
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.max_inflight_messages_set(20)
            self.client.max_queued_messages_set(100)
            self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
            self.client.loop_start()
        except Exception as e:
            print(f"Erreur de connexion MQTT: {e}")
    
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            with self.lock:
                self.connected = True
            print("✓ Connecté au broker MQTT")
        else:
            print(f"✗ Échec de connexion MQTT, code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        with self.lock:
            self.connected = False
        print("✗ Déconnecté du broker MQTT")
        if rc != 0:
            print("⚠ Déconnexion inattendue, tentative de reconnexion...")
    
    def publish_event(self, camera_id, status, metadata=None):
        """Publie un événement caméra sur MQTT avec retry"""
        with self.lock:
            if not self.connected:
                print("⚠ MQTT non connecté, événement non publié")
                return
        
        payload = {
            'camera_id': camera_id,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'metadata': metadata or {}
        }
        
        try:
            result = self.client.publish(MQTT_TOPIC, json.dumps(payload), qos=1, retain=False)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                print(f"📤 Événement publié: Caméra {camera_id} -> {status}")
            else:
                print(f"⚠ Erreur publication MQTT: {result.rc}")
        except Exception as e:
            print(f"Erreur publication MQTT: {e}")
    
    def disconnect(self):
        """Déconnexion propre"""
        self.client.loop_stop()
        self.client.disconnect()

class CameraStreamGStreamer:
    """Stream caméra optimisé avec GStreamer pour Jetson Nano"""
    
    def __init__(self, camera_id):
        self.camera_id = camera_id
        self.camera = None
        self.frame = None
        self.lock = threading.Lock()
        self.is_running = False
        self.thread = None
        self.start_time = None
        self.last_frame_time = None
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        
        # Pipeline GStreamer optimisé pour Jetson
        self.gstreamer_pipeline = self._get_gstreamer_pipeline()
    
    def _get_gstreamer_pipeline(self):
        """
        Pipeline GStreamer optimisé pour Jetson Nano
        Utilise l'accélération matérielle nvvidconv pour la conversion de format
        """
        return (
            f'v4l2src device=/dev/video{self.camera_id} ! '
            'video/x-raw, width=640, height=480, framerate=30/1 ! '
            'videoconvert ! '
            'video/x-raw, format=BGR ! '
            'appsink drop=1 max-buffers=2'
        )
    
    def _get_gstreamer_pipeline_jetson_optimized(self):
        """
        Pipeline GStreamer avec accélération matérielle Jetson (si disponible)
        Nécessite nvvidconv (inclus dans JetPack)
        """
        return (
            f'v4l2src device=/dev/video{self.camera_id} ! '
            'video/x-raw, width=640, height=480, framerate=30/1 ! '
            'nvvidconv ! '  # Accélération matérielle Jetson
            'video/x-raw(memory:NVMM), format=BGRx ! '
            'nvvidconv ! '
            'video/x-raw, format=BGR ! '
            'appsink drop=1 max-buffers=2'
        )
    
    def start(self):
        if self.thread is None or not self.thread.is_alive():
            self.is_running = True
            self.start_time = time.time()
            self.reconnect_attempts = 0
            self.thread = threading.Thread(target=self._capture_frames)
            self.thread.daemon = True
            self.thread.start()
    
    def _capture_frames(self):
        """Capture frames avec OpenCV direct (pas de GStreamer)"""
        while self.is_running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                print(f"📷 Caméra {self.camera_id}: Connexion OpenCV direct...")
            
                # Forcer OpenCV direct (pas de GStreamer)
                self.camera = cv2.VideoCapture(self.camera_id)
            
                if self.camera.isOpened():
                    # Configuration
                    self.camera.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
                    self.camera.set(cv2.CAP_PROP_FRAME_HEIGHT, 480)
                    self.camera.set(cv2.CAP_PROP_FPS, 30)
                    self.camera.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                
                    # Forcer le format pixel MJPEG (compatible)
                    self.camera.set(cv2.CAP_PROP_FOURCC, cv2.VideoWriter_fourcc('M','J','P','G'))
                
                    print(f"✓ Caméra {self.camera_id}: Connectée")
            
                if not self.camera.isOpened():
                    print(f"⚠ Caméra {self.camera_id}: Impossible d'ouvrir")
                    self.reconnect_attempts += 1
                    time.sleep(5)
                    continue
            
                print(f"✓ Caméra {self.camera_id}: Stream démarré")
                self.reconnect_attempts = 0
                consecutive_failures = 0
                max_failures = 30
            
                while self.is_running:
                    ret, frame = self.camera.read()
                
                    if ret and frame is not None:
                        with self.lock:
                            self.frame = frame.copy()
                            self.last_frame_time = time.time()
                        consecutive_failures = 0
                        del frame
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= max_failures:
                            print(f"⚠ Caméra {self.camera_id}: Trop d'échecs ({consecutive_failures}), reconnexion...")
                            break
                
                    time.sleep(0.033)  # ~30 FPS
            
                # Fermer proprement
                if self.camera:
                    self.camera.release()
                    self.camera = None
            
                # Attendre avant reconnexion
                if self.is_running:
                    self.reconnect_attempts += 1
                    time.sleep(2)
                
            except Exception as e:
                print(f"✗ Erreur caméra {self.camera_id}: {e}")
                if self.camera:
                    self.camera.release()
                    self.camera = None
                self.reconnect_attempts += 1
                time.sleep(5)
    
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            print(f"✗ Caméra {self.camera_id}: Trop de tentatives de reconnexion, abandon")

    def get_frame(self):
        """Récupère une frame encodée en JPEG"""
        with self.lock:
            if self.frame is not None:
                if self.last_frame_time and (time.time() - self.last_frame_time) < 5:
                    try:
                        # Encodage JPEG optimisé
                        ret, jpeg = cv2.imencode(
                            '.jpg', 
                            self.frame, 
                            [cv2.IMWRITE_JPEG_QUALITY, 80,
                             cv2.IMWRITE_JPEG_OPTIMIZE, 1]
                        )
                        if ret:
                            return jpeg.tobytes()
                    except Exception as e:
                        print(f"✗ Erreur encodage caméra {self.camera_id}: {e}")
        return None
    
    def stop(self):
        """Arrête proprement le stream"""
        duration = None
        if self.start_time:
            duration = int(time.time() - self.start_time)
        
        self.is_running = False
        
        # Attendre que le thread se termine
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        
        # Libérer la caméra
        if self.camera:
            try:
                self.camera.release()
            except:
                pass
            self.camera = None
        
        # Libérer la frame
        with self.lock:
            self.frame = None
        
        return duration

# Initialisation
db_manager = DatabaseManager(DB_PATH)
mqtt_publisher = MQTTPublisher()
camera_streams = {}

def detect_cameras(max_cameras=10):
    """Détecte les caméras disponibles - méthode robuste"""
    available = []
    
    print("🔍 Détection des caméras...")
    
    # Méthode 1: Vérifier les devices /dev/video*
    import glob
    video_devices = glob.glob('/dev/video*')
    print(f"   Devices trouvés: {video_devices}")
    
    for i in range(max_cameras):
        device_path = f'/dev/video{i}'
        
        # Vérifier que le device existe
        if not os.path.exists(device_path):
            continue
        
        try:
            # Test simple avec OpenCV (fallback si GStreamer échoue)
            cap = cv2.VideoCapture(i)
            if cap.isOpened():
                ret, frame = cap.read()
                if ret and frame is not None:
                    available.append(i)
                    print(f"   ✓ Caméra {i} détectée ({device_path})")
                cap.release()
            
            time.sleep(0.2)
        except Exception as e:
            print(f"   ✗ Erreur test caméra {i}: {e}")
    
    print(f"✓ Détection terminée: {len(available)} caméra(s)")
    return available

available_cameras = detect_cameras()
print(f"🔍 Scan des caméras terminé: {available_cameras}")

for cam_id in available_cameras:
    camera_streams[cam_id] = CameraStreamGStreamer(cam_id)
    camera_streams[cam_id].start()
    
    # Log initial
    db_manager.log_event(cam_id, 'started', 'init')
    mqtt_publisher.publish_event(cam_id, 'started', {'event': 'init'})

def generate_frames(camera_id):
    """Générateur de frames pour le streaming avec timeout"""
    last_frame_time = time.time()
    timeout = 10
    
    while True:
        if camera_id not in camera_streams:
            break
            
        frame = camera_streams[camera_id].get_frame()
        if frame:
            last_frame_time = time.time()
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')
        else:
            if time.time() - last_frame_time > timeout:
                print(f"⚠ Timeout caméra {camera_id}")
                break
        
        time.sleep(0.033)  # ~30 FPS

def cleanup_old_events():
    """Nettoie les anciens événements (>30 jours)"""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cursor = conn.cursor()
        cursor.execute('''
            DELETE FROM camera_events 
            WHERE timestamp < datetime('now', '-30 days')
        ''')
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        if deleted > 0:
            print(f"🧹 {deleted} anciens événements supprimés")
    except Exception as e:
        print(f"⚠ Erreur cleanup: {e}")

def periodic_cleanup():
    """Thread de nettoyage périodique"""
    while True:
        time.sleep(86400)  # Une fois par jour
        cleanup_old_events()
        db_manager.sync_to_grafana()

# Démarrer le thread de nettoyage
cleanup_thread = threading.Thread(target=periodic_cleanup, daemon=True)
cleanup_thread.start()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/cameras')
def get_cameras():
    return jsonify({'cameras': available_cameras})

@app.route('/video_feed/<int:camera_id>')
def video_feed(camera_id):
    if camera_id in camera_streams:
        return Response(generate_frames(camera_id),
                       mimetype='multipart/x-mixed-replace; boundary=frame')
    return "Camera not found", 404

@app.route('/camera/<int:camera_id>/status')
def camera_status(camera_id):
    if camera_id in camera_streams:
        is_active = camera_streams[camera_id].is_running
        return jsonify({
            'status': 'active' if is_active else 'inactive',
            'camera_id': camera_id
        })
    return jsonify({'status': 'not_found'}), 404

@app.route('/camera/<int:camera_id>/toggle', methods=['POST'])
def toggle_camera(camera_id):
    if camera_id not in camera_streams:
        return jsonify({'error': 'Camera not found'}), 404
    
    stream = camera_streams[camera_id]
    
    if stream.is_running:
        duration = stream.stop()
        status = 'stopped'
        
        db_manager.log_event(
            camera_id, 
            status, 
            'manual_stop',
            duration,
            {'user_action': True}
        )
        mqtt_publisher.publish_event(camera_id, status, {
            'duration': duration,
            'action': 'manual_stop'
        })
    else:
        stream.start()
        status = 'started'
        
        db_manager.log_event(
            camera_id,
            status,
            'manual_start',
            metadata={'user_action': True}
        )
        mqtt_publisher.publish_event(camera_id, status, {
            'action': 'manual_start'
        })
    
    return jsonify({
        'status': status,
        'camera_id': camera_id,
        'is_running': stream.is_running
    })

@app.route('/api/stats/<int:camera_id>')
def get_stats(camera_id):
    """Récupère les statistiques d'une caméra"""
    stats = db_manager.get_camera_stats(camera_id)
    return jsonify({
        'camera_id': camera_id,
        'stats': [{'status': s[0], 'count': s[1], 'avg_duration': s[2]} for s in stats]
    })

if __name__ == '__main__':
    try:
        print(f"📹 Caméras détectées: {available_cameras}")
        print(f"💾 Base de données: {DB_PATH}")
        print(f"📊 Base Grafana: {GRAFANA_DB_PATH}")
        print(f"📡 MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"📢 MQTT Topic: {MQTT_TOPIC}")
        print(f"🚀 Mode: GStreamer optimisé pour Jetson Nano")
        
        from werkzeug.serving import run_simple
        run_simple('0.0.0.0', 5000, app, 
                   threaded=True, 
                   use_reloader=False,
                   use_debugger=False)
    except KeyboardInterrupt:
        print("\n⏹ Arrêt en cours...")
    finally:
        print("🧹 Nettoyage des ressources...")
        for stream in camera_streams.values():
            try:
                duration = stream.stop()
                db_manager.log_event(
                    stream.camera_id,
                    'stopped',
                    'shutdown',
                    duration
                )
            except Exception as e:
                print(f"⚠ Erreur arrêt caméra {stream.camera_id}: {e}")
        
        mqtt_publisher.disconnect()
        print("✓ Arrêt propre terminé")