"""
Configuration management for Home Assistant to InfluxDB exporter.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
from dotenv import load_dotenv


class Config:
    """Configuration management with validation."""
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize configuration from environment variables."""
        if env_file:
            load_dotenv(env_file)
        else:
            # Look for .env file in project root
            env_path = Path(__file__).parent.parent / '.env'
            if env_path.exists():
                load_dotenv(env_path)
        
        self._validate_config()
    
    # Database Configuration
    @property
    def ha_database_path(self) -> str:
        """Path to Home Assistant SQLite database."""
        return os.getenv('HA_DATABASE_PATH', './home-assistant_v2.db')
    
    # InfluxDB Configuration
    @property
    def influx_url(self) -> str:
        """InfluxDB server URL."""
        return os.getenv('INFLUX_URL', 'http://localhost:8086')
    
    @property
    def influx_token(self) -> str:
        """InfluxDB authentication token."""
        token = os.getenv('INFLUX_TOKEN')
        if not token:
            raise ValueError("INFLUX_TOKEN environment variable is required")
        return token
    
    @property
    def influx_org(self) -> str:
        """InfluxDB organization."""
        org = os.getenv('INFLUX_ORG')
        if not org:
            raise ValueError("INFLUX_ORG environment variable is required")
        return org
    
    @property
    def influx_bucket_recent(self) -> str:
        """InfluxDB bucket for recent data (90 days)."""
        return os.getenv('INFLUX_BUCKET_RECENT', 'homeassistant-recent')
    
    @property
    def influx_bucket_historical(self) -> str:
        """InfluxDB bucket for historical data (unlimited)."""
        return os.getenv('INFLUX_BUCKET_HISTORICAL', 'homeassistant-historical')
    
    # Processing Configuration
    @property
    def batch_size(self) -> int:
        """Number of records to process in each batch."""
        return int(os.getenv('BATCH_SIZE', '1000'))
    
    @property
    def progress_interval(self) -> int:
        """Interval for progress reporting (number of batches)."""
        return int(os.getenv('PROGRESS_INTERVAL', '10'))
    
    @property
    def resume_enabled(self) -> bool:
        """Enable resume functionality."""
        return os.getenv('RESUME_ENABLED', 'true').lower() == 'true'
    
    @property
    def checkpoint_file(self) -> str:
        """Path to checkpoint file for resume functionality."""
        return os.getenv('CHECKPOINT_FILE', './export_checkpoint.json')
    
    # Logging Configuration
    @property
    def log_level(self) -> str:
        """Logging level."""
        return os.getenv('LOG_LEVEL', 'INFO').upper()
    
    @property
    def log_file(self) -> Optional[str]:
        """Log file path (optional)."""
        return os.getenv('LOG_FILE')
    
    # Entity Filtering Configuration
    @property
    def include_units(self) -> List[str]:
        """Units of measurement to include in export."""
        units = os.getenv('INCLUDE_UNITS', 'kWh,W,°C,°F,kB/s,GB,MB,A,V,hPa,bar,mbar,lux,ppm,dB,rpm')
        return [unit.strip() for unit in units.split(',')]
    
    @property
    def include_sources(self) -> List[str]:
        """Sources to include regardless of unit."""
        sources = os.getenv('INCLUDE_SOURCES', 'tibber')
        return [source.strip() for source in sources.split(',')]
    
    @property
    def exclude_patterns(self) -> List[str]:
        """Entity ID patterns to exclude."""
        patterns = os.getenv('EXCLUDE_PATTERNS', '%availability%,%status%,%signal%,%connected%')
        return [pattern.strip() for pattern in patterns.split(',')]
    
    @property
    def include_domains(self) -> List[str]:
        """Entity domains to include in export."""
        domains = os.getenv('INCLUDE_DOMAINS', 'sensor,counter,weather,climate,utility_meter')
        return [domain.strip() for domain in domains.split(',')]
    
    # Data Quality Configuration
    @property
    def quality_rules(self) -> Dict[str, Dict[str, Optional[float]]]:
        """Data quality validation rules by unit."""
        return {
            '°C': {'min': -50, 'max': 80},
            'W': {'min': 0, 'max': 50000},
            'kWh': {'min': 0, 'max': None},
            '%': {'min': 0, 'max': 100},
            'V': {'min': 0, 'max': 500},
            'A': {'min': 0, 'max': 1000},
        }
    
    def _validate_config(self):
        """Validate required configuration parameters."""
        required_vars = ['INFLUX_TOKEN', 'INFLUX_ORG']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        # Validate database path exists
        if not Path(self.ha_database_path).exists():
            raise FileNotFoundError(f"Home Assistant database not found: {self.ha_database_path}")
        
        # Validate batch size
        if self.batch_size <= 0:
            raise ValueError("BATCH_SIZE must be greater than 0")
    
    def setup_logging(self):
        """Set up logging configuration."""
        level = getattr(logging, self.log_level, logging.INFO)
        
        # Configure logging format
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        handlers = [console_handler]
        
        # File handler if specified
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        
        # Configure root logger
        logging.basicConfig(
            level=level,
            handlers=handlers,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def print_summary(self):
        """Print configuration summary (excluding sensitive data)."""
        print("Configuration Summary:")
        print(f"  HA Database: {self.ha_database_path}")
        print(f"  InfluxDB URL: {self.influx_url}")
        print(f"  InfluxDB Org: {self.influx_org}")
        print(f"  Recent Bucket: {self.influx_bucket_recent}")
        print(f"  Historical Bucket: {self.influx_bucket_historical}")
        print(f"  Batch Size: {self.batch_size}")
        print(f"  Resume Enabled: {self.resume_enabled}")
        print(f"  Include Units: {', '.join(self.include_units)}")
        print(f"  Log Level: {self.log_level}")


# Global configuration instance
config = Config()