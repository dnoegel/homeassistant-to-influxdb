"""
InfluxDB client for Home Assistant statistics export.
"""

import logging
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Any
from dataclasses import dataclass
import time

from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from src.config import config
from src.database import StatisticRecord, StatisticMetadata
from src.entity_filter import FilteredEntity, SensorCategory


logger = logging.getLogger(__name__)


@dataclass
class WriteResult:
    """Result of a write operation."""
    success: bool
    points_written: int
    errors: List[str]
    duration_seconds: float


class InfluxDBManager:
    """Manages InfluxDB operations for Home Assistant statistics."""
    
    def __init__(self):
        """Initialize InfluxDB manager."""
        self.client = None
        self.write_api = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize InfluxDB client and APIs."""
        try:
            self.client = InfluxDBClient(
                url=config.influx_url,
                token=config.influx_token,
                org=config.influx_org,
                timeout=config.influx_timeout * 1000,  # Convert to milliseconds
                retries=3
            )
            
            # Configure write API with batching for better performance
            write_options = WriteOptions(
                batch_size=config.batch_size,
                flush_interval=5_000,   # 5 seconds (faster flushing)
                jitter_interval=1_000,   # 1 second (reduce jitter)
                retry_interval=2_000,    # 2 seconds (faster retries)
                max_retries=3
            )
            
            self.write_api = self.client.write_api(write_options=write_options)
            
            # Verify connection
            self._verify_connection()
            logger.info("InfluxDB client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize InfluxDB client: {e}")
            raise
    
    def _verify_connection(self):
        """Verify InfluxDB connection and permissions."""
        try:
            # Test connection by checking health
            health = self.client.health()
            if health.status != "pass":
                raise ConnectionError(f"InfluxDB health check failed: {health.message}")
            
            # Verify organization exists
            orgs_api = self.client.organizations_api()
            try:
                org = orgs_api.find_organizations(org=config.influx_org)
                if not org:
                    raise ValueError(f"Organization '{config.influx_org}' not found")
                logger.info(f"Connected to InfluxDB org: {config.influx_org}")
            except ApiException as e:
                raise PermissionError(f"Cannot access organization: {e}")
                
        except Exception as e:
            raise ConnectionError(f"InfluxDB connection verification failed: {e}")
    
    def create_bucket(self, bucket_name: str, retention_seconds: int = 0, 
                     description: str = "") -> bool:
        """
        Create a bucket if it doesn't exist.
        
        Args:
            bucket_name: Name of the bucket
            retention_seconds: Retention period (0 = infinite)
            description: Bucket description
            
        Returns:
            True if bucket was created or already exists
        """
        try:
            buckets_api = self.client.buckets_api()
            
            # Check if bucket already exists
            try:
                bucket = buckets_api.find_bucket_by_name(bucket_name)
                if bucket:
                    logger.info(f"Bucket '{bucket_name}' already exists")
                    return True
            except ApiException:
                pass  # Bucket doesn't exist, proceed with creation
            
            # Create bucket
            from influxdb_client.domain.bucket import Bucket
            from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules
            
            # Set up retention rules
            retention_rules = []
            if retention_seconds > 0:
                retention_rule = BucketRetentionRules(
                    type="expire",
                    every_seconds=retention_seconds
                )
                retention_rules.append(retention_rule)
            
            bucket = Bucket(
                name=bucket_name,
                description=description,
                retention_rules=retention_rules,
                org_id=self._get_org_id()
            )
            
            created_bucket = buckets_api.create_bucket(bucket=bucket)
            logger.info(f"Created bucket '{bucket_name}' with retention {retention_seconds}s")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create bucket '{bucket_name}': {e}")
            return False
    
    def _get_org_id(self) -> str:
        """Get organization ID for the configured organization."""
        orgs_api = self.client.organizations_api()
        orgs = orgs_api.find_organizations(org=config.influx_org)
        if not orgs:
            raise ValueError(f"Organization '{config.influx_org}' not found")
        return orgs[0].id
    
    def bucket_exists(self, bucket_name: str) -> bool:
        """Check if a bucket exists."""
        try:
            buckets_api = self.client.buckets_api()
            bucket = buckets_api.find_bucket_by_name(bucket_name)
            return bucket is not None
        except Exception:
            return False
    
    def create_point(self, 
                    entity: FilteredEntity,
                    record: StatisticRecord,
                    measurement_name: Optional[str] = None) -> Optional[Point]:
        """
        Create an InfluxDB point from a statistic record.
        
        Args:
            entity: Filtered entity with metadata
            record: Statistic record
            measurement_name: InfluxDB measurement name
            
        Returns:
            InfluxDB Point or None if invalid
        """
        try:
            # Determine measurement name: use unit_of_measurement or fallback
            if measurement_name is None:
                if entity.metadata.unit_of_measurement:
                    measurement_name = entity.metadata.unit_of_measurement
                else:
                    # Fallback for entities without unit
                    measurement_name = f"{entity.metadata.statistic_id.split('.')[0]}_data"
            
            # Create point with timestamp  
            timestamp = datetime.fromtimestamp(record.start_ts, tz=timezone.utc)
            point = Point(measurement_name).time(timestamp, WritePrecision.S)
            
            # Add tags (indexed for efficient filtering/grouping)
            domain, entity_id = entity.metadata.statistic_id.split(".", 1)

            point.tag("entity_id", entity_id)
            point.tag("domain", domain)
            point.tag("category", entity.category.value)
            
            if entity.metadata.unit_of_measurement:
                point.tag("unit", entity.metadata.unit_of_measurement)
            
            # Use "migration" as source to distinguish from live HA data
            point.tag("source", "migration")
            
            # FIXED: Add metadata as TAGS (not fields) to match HA native schema
            if entity.metadata.friendly_name:
                point.tag("friendly_name", entity.metadata.friendly_name)
                
            if entity.metadata.device_class:
                point.tag("device_class", entity.metadata.device_class)
            
            # Add fields for actual time-series data
            fields_added = 0
            
            # Always try to add value if available (current value)
            if record.value is not None and self._is_valid_value(record.value):
                point.field("value", float(record.value))
                fields_added += 1
            # Fallback: use mean as value for statistics data when state is NULL
            elif record.mean is not None and self._is_valid_value(record.mean):
                point.field("value", float(record.mean))
                fields_added += 1
            
            # NOTE: Removed statistical fields (mean, min, max, sum) for consistency with HA native integration
            # HA only writes 'value' field, so we match that schema for unified querying
            
            # Only return point if we have at least one field
            if fields_added == 0:
                logger.debug(f"Skipping point for {entity.metadata.statistic_id}: no valid fields")
                return None
            
            return point
            
        except Exception as e:
            logger.error(f"Failed to create point for {entity.metadata.statistic_id}: {e}")
            return None
    
    def _is_valid_value(self, value: float) -> bool:
        """Check if a value is valid for InfluxDB."""
        import math
        return not (math.isnan(value) or math.isinf(value))
    
    def write_points(self, points: List[Point], bucket: str) -> WriteResult:
        """
        Write points to InfluxDB.
        
        Args:
            points: List of InfluxDB points
            bucket: Target bucket name
            
        Returns:
            WriteResult with status and metrics
        """
        start_time = time.time()
        errors = []
        
        try:
            if not points:
                return WriteResult(True, 0, [], 0.0)
            
            # Write points
            self.write_api.write(bucket=bucket, record=points)
            
            duration = time.time() - start_time
            logger.debug(f"Wrote {len(points)} points to '{bucket}' in {duration:.2f}s")
            
            return WriteResult(
                success=True,
                points_written=len(points),
                errors=[],
                duration_seconds=duration
            )
            
        except Exception as e:
            duration = time.time() - start_time
            error_msg = f"Failed to write {len(points)} points to '{bucket}': {e}"
            logger.error(error_msg)
            errors.append(error_msg)
            
            return WriteResult(
                success=False,
                points_written=0,
                errors=errors,
                duration_seconds=duration
            )
    
    def write_statistics_batch(self,
                             entities: List[FilteredEntity],
                             records: List[StatisticRecord],
                             bucket: str,
                             metadata_lookup: Dict[int, StatisticMetadata]) -> WriteResult:
        """
        Write a batch of statistics records to InfluxDB.
        
        Args:
            entities: List of filtered entities
            records: List of statistic records
            bucket: Target bucket
            metadata_lookup: Mapping of metadata_id to metadata
            
        Returns:
            WriteResult with batch status
        """
        start_time = time.time()
        
        # Create entity lookup for faster access
        entity_lookup = {entity.metadata.id: entity for entity in entities}
        
        points = []
        skipped = 0
        
        for record in records:
            entity = entity_lookup.get(record.metadata_id)
            if not entity:
                skipped += 1
                continue
            
            point = self.create_point(entity, record)
            if point:
                points.append(point)
            else:
                skipped += 1
        
        if skipped > 0:
            logger.debug(f"Skipped {skipped} records without valid data")
        
        # Write points
        result = self.write_points(points, bucket)
        
        # Update metrics
        total_duration = time.time() - start_time
        logger.info(f"Batch write: {result.points_written}/{len(records)} points to '{bucket}' "
                   f"in {total_duration:.2f}s (skipped: {skipped})")
        
        return WriteResult(
            success=result.success,
            points_written=result.points_written,
            errors=result.errors,
            duration_seconds=total_duration
        )
    
    def query_data(self, bucket: str, query: str) -> List[Dict]:
        """
        Execute a Flux query against InfluxDB.
        
        Args:
            bucket: Bucket to query
            query: Flux query string
            
        Returns:
            Query results as list of dictionaries
        """
        try:
            query_api = self.client.query_api()
            tables = query_api.query(query)
            
            results = []
            for table in tables:
                for record in table.records:
                    results.append(record.values)
            
            return results
            
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def get_bucket_stats(self, bucket: str) -> Dict[str, Any]:
        """Get statistics about a bucket."""
        try:
            # Query for basic stats
            query = f'''
            from(bucket: "{bucket}")
              |> range(start: -30d)
              |> group()
              |> count()
            '''
            
            results = self.query_data(bucket, query)
            
            # Extract count
            total_points = 0
            if results:
                total_points = results[0].get('_value', 0)
            
            return {
                'bucket': bucket,
                'total_points': total_points,
                'query_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get bucket stats: {e}")
            return {'bucket': bucket, 'error': str(e)}
    
    def close(self):
        """Close InfluxDB connections."""
        try:
            if self.write_api:
                self.write_api.close()
            if self.client:
                self.client.close()
            logger.info("InfluxDB client closed")
        except Exception as e:
            logger.error(f"Error closing InfluxDB client: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global InfluxDB manager instance (lazy initialization)
_influx_instance = None

def get_influx_manager() -> InfluxDBManager:
    """Get the global InfluxDB manager instance with lazy initialization."""
    global _influx_instance
    if _influx_instance is None:
        _influx_instance = InfluxDBManager()
    return _influx_instance