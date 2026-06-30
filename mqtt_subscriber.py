#!/usr/bin/env python3
"""
MQTT Subscriber pour enregistrer l'activité des caméras
"""
import paho.mqtt.client as mqtt
import sqlite3
import json
import os
import shutil
from datetime import datetime
import time

# Configuration
DB_PATH = '/home/nemo/Documents/CameraVisual/camera_logs/camera_activity.db'
GRAFANA_DB_PATH = '/home/nemo/Documents/CameraVisual/grafana_data/camera_activity.db'
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC = 'cameras/status'
SYNC_INTERVAL = 30  # Synchronisation toutes les 30 secondes

class CameraEventSubscriber:
    def __init__(self):
        self.client = mqtt.Client()
        self.db_path = DB_PATH
        self.grafana_db_path = GRAFANA_DB_PATH
        self.last_sync = time.time()
        self.init_database()
        self.setup_mqtt()
    
    def init_database(self):
        """Initialise la base de données SQLite"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        os.makedirs(os.path.dirname(self.grafana_db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Table principale des événements
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
        
        # Table agrégée pour Grafana (vue optimisée)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS camera_stats_hourly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hour_timestamp DATETIME NOT NULL,
                camera_id INTEGER NOT NULL,
                total_events INTEGER DEFAULT 0,
                started_count INTEGER DEFAULT 0,
                stopped_count INTEGER DEFAULT 0,
                total_uptime_seconds INTEGER DEFAULT 0,
                avg_session_duration REAL DEFAULT 0,
                UNIQUE(hour_timestamp, camera_id)
            )
        ''')
        
        # Index pour performances
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_camera_timestamp 
            ON camera_events(camera_id, timestamp)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON camera_events(timestamp)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_stats_hour 
            ON camera_stats_hourly(hour_timestamp, camera_id)
        ''')
        
        conn.commit()
        conn.close()
        
        print("✓ Base de données initialisée")
    
    def setup_mqtt(self):
        """Configure le client MQTT"""
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        try:
            self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
            print(f"✓ Connexion au broker MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        except Exception as e:
            print(f"✗ Erreur de connexion MQTT: {e}")
            raise
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback de connexion MQTT"""
        if rc == 0:
            print("✓ Connecté au broker MQTT")
            client.subscribe(MQTT_TOPIC)
            print(f"✓ Abonné au topic: {MQTT_TOPIC}")
        else:
            print(f"✗ Échec de connexion MQTT, code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback de déconnexion MQTT"""
        print(f"✗ Déconnecté du broker MQTT (code: {rc})")
        if rc != 0:
            print("⚠ Déconnexion inattendue, tentative de reconnexion...")
    
    def on_message(self, client, userdata, msg):
        """Callback de réception de message MQTT"""
        try:
            payload = json.loads(msg.payload.decode())
            camera_id = payload.get('camera_id')
            status = payload.get('status')
            timestamp = payload.get('timestamp')
            metadata = payload.get('metadata', {})
            
            print(f"📥 Message reçu - Caméra {camera_id}: {status}")
            
            # Enregistrer dans la base de données
            self.save_event(camera_id, status, metadata)
            
            # Mettre à jour les statistiques agrégées
            self.update_hourly_stats(camera_id, status, metadata)
            
            # Synchroniser avec Grafana si nécessaire
            if time.time() - self.last_sync > SYNC_INTERVAL:
                self.sync_to_grafana()
                self.last_sync = time.time()
            
        except json.JSONDecodeError as e:
            print(f"✗ Erreur de décodage JSON: {e}")
        except Exception as e:
            print(f"✗ Erreur lors du traitement du message: {e}")
    
    def save_event(self, camera_id, status, metadata):
        """Enregistre un événement dans la base de données"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        duration = metadata.get('duration')
        event_type = metadata.get('action', 'unknown')
        
        cursor.execute('''
            INSERT INTO camera_events 
            (camera_id, status, event_type, duration_seconds, metadata)
            VALUES (?, ?, ?, ?, ?)
        ''', (camera_id, status, event_type, duration, json.dumps(metadata)))
        
        conn.commit()
        conn.close()
        
        print(f"💾 Événement enregistré: Caméra {camera_id} -> {status}")
    
    def update_hourly_stats(self, camera_id, status, metadata):
        """Met à jour les statistiques horaires agrégées"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
    
        # Arrondir à l'heure
        now = datetime.now()
        hour_timestamp = now.replace(minute=0, second=0, microsecond=0)
    
        duration = metadata.get('duration', 0) or 0
    
        # Vérifier si l'entrée existe
        cursor.execute('''
            SELECT id FROM camera_stats_hourly 
            WHERE hour_timestamp = ? AND camera_id = ?
        ''', (hour_timestamp, camera_id))
    
        exists = cursor.fetchone()
    
        if exists:
         # Mettre à jour
            cursor.execute('''
                UPDATE camera_stats_hourly SET
                    total_events = total_events + 1,
                    started_count = started_count + ?,
                    stopped_count = stopped_count + ?,
                    total_uptime_seconds = total_uptime_seconds + ?
                WHERE hour_timestamp = ? AND camera_id = ?
            ''', (
                1 if status == 'started' else 0,
                0 if status == 'started' else 1,
                duration,
                hour_timestamp,
                camera_id
            ))
        else:
            # Insérer
            cursor.execute('''
                INSERT INTO camera_stats_hourly 
                (hour_timestamp, camera_id, total_events, started_count, stopped_count, total_uptime_seconds)
                VALUES (?, ?, 1, ?, ?, ?)
            ''', (
                hour_timestamp,
                camera_id,
                1 if status == 'started' else 0,
                0 if status == 'started' else 1,
                duration
            ))
    
        # Calculer la durée moyenne
        cursor.execute('''
            UPDATE camera_stats_hourly
            SET avg_session_duration = CAST(total_uptime_seconds AS REAL) / NULLIF(stopped_count, 0)
            WHERE hour_timestamp = ? AND camera_id = ?
        ''', (hour_timestamp, camera_id))
    
        conn.commit()
        conn.close()
    
    def sync_to_grafana(self):
        """Synchronise la base de données avec la copie Grafana"""
        try:
            shutil.copy2(self.db_path, self.grafana_db_path)
            print(f"✓ Base synchronisée vers Grafana: {self.grafana_db_path}")
        except Exception as e:
            print(f"✗ Erreur de synchronisation Grafana: {e}")
    
    def run(self):
        """Lance le subscriber en boucle infinie"""
        print("🚀 Subscriber démarré")
        print(f"📊 Base de données: {self.db_path}")
        print(f"📈 Base Grafana: {self.grafana_db_path}")
        print(f"🔄 Synchronisation toutes les {SYNC_INTERVAL}s")
        print("-" * 50)
        
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("\n⏹ Arrêt du subscriber...")
            self.sync_to_grafana()  # Dernière synchro
            self.client.disconnect()
            print("✓ Subscriber arrêté proprement")

if __name__ == '__main__':
    subscriber = CameraEventSubscriber()
    subscriber.run()