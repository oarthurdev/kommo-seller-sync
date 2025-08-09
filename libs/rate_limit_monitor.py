
import logging
import time
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading

logger = logging.getLogger(__name__)

class RateLimitMonitor:
    def __init__(self, max_retries=3, initial_backoff=2):
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.request_counts = defaultdict(int)
        self.last_reset = datetime.now()
        self.error_counts = defaultdict(int)
        self.reset_interval = timedelta(minutes=15)
        
        # Rate limiting para Kommo API: máximo 7 solicitações por segundo
        self.max_requests_per_second = 7
        self.request_times = deque()
        self.lock = threading.Lock()

    def reset_counts(self):
        if datetime.now() - self.last_reset > self.reset_interval:
            self.request_counts.clear()
            self.error_counts.clear()
            self.last_reset = datetime.now()

    def calculate_backoff(self, attempt, endpoint):
        """Calculate backoff time with jitter"""
        base_delay = min(300, self.initial_backoff * (2 ** attempt))  # Cap at 5 minutes
        jitter = base_delay * 0.1  # 10% jitter
        return base_delay + ((-jitter/2) + (time.time() % jitter))

    def should_retry(self, endpoint, status_code):
        self.reset_counts()
        
        # Increment error count
        if status_code in (429, 403):
            self.error_counts[endpoint] += 1
        
        # Check if we should stop retrying
        if self.error_counts[endpoint] > self.max_retries:
            logger.error(f"Maximum retries exceeded for endpoint {endpoint}")
            return False
            
        return True

    def wait_before_retry(self, endpoint, attempt):
        backoff = self.calculate_backoff(attempt, endpoint)
        logger.warning(f"Rate limit hit for {endpoint}. Waiting {backoff:.2f}s before retry (attempt {attempt + 1}/{self.max_retries})")
        time.sleep(backoff)
    
    def enforce_rate_limit(self):
        """Aplica o rate limiting rigoroso de 7 solicitações por segundo conforme documentação Kommo"""
        with self.lock:
            now = time.time()
            
            # Remove requests antigas (mais de 1 segundo)
            while self.request_times and now - self.request_times[0] > 1.0:
                self.request_times.popleft()
            
            # Se já temos 7 requests no último segundo, espera obrigatoriamente
            if len(self.request_times) >= self.max_requests_per_second:
                # Calcula o tempo necessário para esperar
                oldest_request_time = self.request_times[0]
                time_since_oldest = now - oldest_request_time
                sleep_time = 1.0 - time_since_oldest + 0.01  # +10ms de margem de segurança
                
                if sleep_time > 0:
                    logger.info(f"Rate limit: waiting {sleep_time:.3f}s to respect 7 req/s limit")
                    time.sleep(sleep_time)
                    # Atualiza o tempo atual após o sleep
                    now = time.time()
                    
                    # Remove requests antigas novamente após o sleep
                    while self.request_times and now - self.request_times[0] > 1.0:
                        self.request_times.popleft()
            
            # Adiciona delay mínimo entre requests para distribuir melhor
            min_interval = 1.0 / self.max_requests_per_second  # ~0.143s entre requests
            if self.request_times:
                time_since_last = now - self.request_times[-1]
                if time_since_last < min_interval:
                    additional_sleep = min_interval - time_since_last
                    logger.debug(f"Adding minimum interval delay: {additional_sleep:.3f}s")
                    time.sleep(additional_sleep)
                    now = time.time()
            
            # Registra a nova request
            self.request_times.append(now)
    
    def handle_kommo_error(self, status_code, endpoint, attempt):
        """Trata códigos de erro específicos da API Kommo"""
        if status_code == 429:
            # Código HTTP 429: excesso de solicitações
            logger.warning(f"HTTP 429 - Rate limit exceeded for {endpoint}")
            return self.should_retry(endpoint, status_code)
        
        elif status_code == 403:
            # Código HTTP 403: IP bloqueado
            logger.error(f"HTTP 403 - IP blocked for {endpoint}. Check API restrictions.")
            return False  # Não tenta novamente para IP bloqueado
        
        elif status_code == 504:
            # Código HTTP 504: Reduzir número de entidades na solicitação
            logger.warning(f"HTTP 504 - Gateway timeout for {endpoint}. Consider reducing batch size.")
            return self.should_retry(endpoint, status_code)
        
        return True
