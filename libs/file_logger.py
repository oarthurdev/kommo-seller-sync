
import os
import logging
import threading
from datetime import datetime
from pathlib import Path

class SyncFileLogger:
    def __init__(self, log_file="sync_logs.txt", max_size_mb=5):
        self.log_file = log_file
        self.max_size_bytes = max_size_mb * 1024 * 1024  # 5MB
        self.lock = threading.Lock()
        
        # Ensure the log file exists
        Path(self.log_file).touch(exist_ok=True)
        
    def _check_file_size(self):
        """Check if log file exceeds max size and rotate if needed"""
        try:
            if os.path.exists(self.log_file):
                size = os.path.getsize(self.log_file)
                if size > self.max_size_bytes:
                    # Keep only the last 30% of the file
                    with open(self.log_file, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    
                    # Keep last 30% of lines
                    keep_lines = int(len(lines) * 0.3)
                    if keep_lines > 0:
                        with open(self.log_file, 'w', encoding='utf-8') as f:
                            f.write(f"=== LOG ROTATED AT {datetime.now().isoformat()} ===\n")
                            f.writelines(lines[-keep_lines:])
                    else:
                        # If too few lines, just clear the file
                        with open(self.log_file, 'w', encoding='utf-8') as f:
                            f.write(f"=== LOG CLEARED AT {datetime.now().isoformat()} ===\n")
        except Exception as e:
            print(f"Error rotating log file: {e}")
    
    def log_sync_start(self, company_id, sync_type="incremental"):
        """Log when sync starts for a company"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] SYNC_START - Company: {company_id}, Type: {sync_type}\n")
    
    def log_sync_complete(self, company_id, changes_detected, duration_ms=None):
        """Log when sync completes with summary"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            total_changes = sum(1 for changed in changes_detected.values() if changed)
            duration_str = f", Duration: {duration_ms}ms" if duration_ms else ""
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] SYNC_COMPLETE - Company: {company_id}, Changes: {total_changes}{duration_str}\n")
                if total_changes > 0:
                    change_details = ", ".join([k for k, v in changes_detected.items() if v])
                    f.write(f"                   Changed: {change_details}\n")
    
    def log_sync_error(self, company_id, error_msg):
        """Log sync errors"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] SYNC_ERROR - Company: {company_id}, Error: {error_msg}\n")
    
    def log_data_volume(self, company_id, brokers=0, leads=0, activities=0, stages=0):
        """Log data volumes being processed"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] DATA_VOLUME - Company: {company_id}, "
                       f"Brokers: {brokers}, Leads: {leads}, Activities: {activities}, Stages: {stages}\n")
    
    def log_webhook_received(self, webhook_type, payload_id=None, company_info=None):
        """Log important webhook events"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            company_str = f", Company: {company_info}" if company_info else ""
            payload_str = f", ID: {payload_id}" if payload_id else ""
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] WEBHOOK - Type: {webhook_type}{payload_str}{company_str}\n")
    
    def log_api_error(self, company_id, endpoint, error_code, retry_count=None):
        """Log API errors and retries"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            retry_str = f", Retry: {retry_count}" if retry_count is not None else ""
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] API_ERROR - Company: {company_id}, "
                       f"Endpoint: {endpoint}, Code: {error_code}{retry_str}\n")
    
    def log_broker_points_update(self, company_id, brokers_updated, leads_processed):
        """Log broker points calculation"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] POINTS_UPDATE - Company: {company_id}, "
                       f"Brokers: {brokers_updated}, Leads: {leads_processed}\n")
    
    def log_system_event(self, event_type, message, company_id=None):
        """Log general system events"""
        with self.lock:
            self._check_file_size()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            company_str = f", Company: {company_id}" if company_id else ""
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(f"[{timestamp}] {event_type.upper()} - {message}{company_str}\n")

# Global logger instance
sync_file_logger = SyncFileLogger()
