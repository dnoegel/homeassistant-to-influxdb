"""
Home Assistant SQLite database interface.
"""

import sqlite3
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Iterator, Any
from contextlib import contextmanager
from dataclasses import dataclass

from src.config import config


logger = logging.getLogger(__name__)


@dataclass
class StatisticMetadata:
    """Represents metadata for a statistic entity."""
    id: int
    statistic_id: str
    source: str
    unit_of_measurement: Optional[str]
    has_mean: bool
    has_sum: bool
    name: Optional[str]
    mean_type: int
    unit_class: Optional[str]
    friendly_name: Optional[str] = None
    device_class: Optional[str] = None
    state_class: Optional[str] = None


@dataclass
class StatisticRecord:
    """Represents a statistic record."""
    id: int
    created_ts: float
    metadata_id: int
    start_ts: float
    mean: Optional[float]
    mean_weight: Optional[float]
    min_value: Optional[float]
    max_value: Optional[float]
    last_reset_ts: Optional[float]
    value: Optional[float]
    sum_value: Optional[float]


class DatabaseInterface:
    """Interface for Home Assistant SQLite database operations."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize database interface."""
        self.db_path = db_path or config.ha_database_path
        self._validate_database()
    
    def _validate_database(self):
        """Validate that the database exists and is accessible."""
        if not Path(self.db_path).exists():
            raise FileNotFoundError(f"Database file not found: {self.db_path}")
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Test basic connectivity and verify required tables exist
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = ['statistics_meta', 'statistics', 'statistics_short_term']
                missing_tables = [table for table in required_tables if table not in tables]
                
                if missing_tables:
                    raise ValueError(f"Required tables missing: {missing_tables}")
                
                logger.info(f"Database validation successful: {self.db_path}")
                
        except sqlite3.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with proper resource management."""
        conn = sqlite3.connect(self.db_path)
        try:
            # Configure connection for better performance and consistency
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA temp_store=MEMORY")
            yield conn
        finally:
            conn.close()
    
    def get_statistics_metadata(self) -> List[StatisticMetadata]:
        """Retrieve all statistics metadata with enhanced attributes - OPTIMIZED."""
        # Check if states_meta table exists
        check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='states_meta'"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(check_query)
            has_states_meta = cursor.fetchone() is not None

            if has_states_meta:
                # OPTIMIZED: Subquery to get only latest attributes per entity
                logger.info("Using optimized subquery for metadata loading")

                query = """
                SELECT sm.id, sm.statistic_id, sm.source, sm.unit_of_measurement,
                       sm.has_mean, sm.has_sum, sm.name, sm.mean_type, sm.unit_class,
                       COALESCE(JSON_EXTRACT(sa.shared_attrs, '$.friendly_name'),
                               REPLACE(REPLACE(sm.statistic_id, 'sensor.', ''), '_', ' ')) as friendly_name,
                       JSON_EXTRACT(sa.shared_attrs, '$.device_class') as device_class,
                       JSON_EXTRACT(sa.shared_attrs, '$.state_class') as state_class
                FROM statistics_meta sm
                LEFT JOIN states_meta stm ON sm.statistic_id = stm.entity_id
                LEFT JOIN (
                    SELECT s.metadata_id, s.attributes_id
                    FROM states s
                    INNER JOIN (
                        SELECT metadata_id, MAX(last_updated_ts) as max_ts
                        FROM states
                        WHERE attributes_id IS NOT NULL
                        GROUP BY metadata_id
                    ) latest_ts ON s.metadata_id = latest_ts.metadata_id
                                AND s.last_updated_ts = latest_ts.max_ts
                    WHERE s.attributes_id IS NOT NULL
                    GROUP BY s.metadata_id
                ) latest ON stm.metadata_id = latest.metadata_id
                LEFT JOIN state_attributes sa ON latest.attributes_id = sa.attributes_id
                """

                logger.info("Optimized join: uses subquery for latest attributes only")
            else:
                # Fallback query without states_meta
                logger.warning("states_meta table not found, using basic metadata")
                query = """
                SELECT id, statistic_id, source, unit_of_measurement, has_mean, has_sum,
                       name, mean_type, unit_class, name, NULL, NULL
                FROM statistics_meta
                """

            cursor.execute(query)

            metadata_list = []
            for row in cursor.fetchall():
                # Filter out timestamp-only entities
                state_class = row[11] if len(row) > 11 else None
                if state_class == 'timestamp':
                    logger.debug(f"Skipping timestamp-only entity: {row[1]}")
                    continue

                metadata = StatisticMetadata(
                    id=row[0],
                    statistic_id=row[1],
                    source=row[2],
                    unit_of_measurement=row[3],
                    has_mean=bool(row[4]),
                    has_sum=bool(row[5]),
                    name=row[6],
                    mean_type=row[7],
                    unit_class=row[8],
                    friendly_name=row[9] if len(row) > 9 else None,
                    device_class=row[10] if len(row) > 10 else None,
                    state_class=state_class
                )
                metadata_list.append(metadata)

            logger.info(f"Retrieved {len(metadata_list)} unique entities (optimized)")
            return metadata_list
    
    def get_statistics_metadata_latest_only(self) -> List[StatisticMetadata]:
        """Retrieve latest statistics metadata record per entity for fast processing."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # First, get latest metadata records (guaranteed 1 per entity)
            base_query = """
            SELECT sm.id, sm.statistic_id, sm.source, sm.unit_of_measurement,
                   sm.has_mean, sm.has_sum, sm.name, sm.mean_type, sm.unit_class
            FROM statistics_meta sm
            WHERE sm.id IN (
                SELECT MAX(id) FROM statistics_meta GROUP BY statistic_id
            )
            ORDER BY sm.statistic_id
            """
            
            cursor.execute(base_query)
            base_rows = cursor.fetchall()
            
            logger.info(f"Retrieved {len(base_rows)} latest metadata records (1 per entity)")
            
            # Check if states_meta table exists for attribute lookup
            check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='states_meta'"
            cursor.execute(check_query)
            has_states_meta = cursor.fetchone() is not None
            
            # Batch lookup attributes if available
            attribute_map = {}
            if has_states_meta:
                entity_ids = [row[1] for row in base_rows]
                if entity_ids:
                    # Batch query for attributes (similar to earlier optimization)
                    placeholders = ','.join('?' * len(entity_ids))
                    attr_query = f"""
                    SELECT stm.entity_id,
                           JSON_EXTRACT(sa.shared_attrs, '$.friendly_name') as friendly_name,
                           JSON_EXTRACT(sa.shared_attrs, '$.device_class') as device_class,
                           JSON_EXTRACT(sa.shared_attrs, '$.state_class') as state_class
                    FROM states_meta stm
                    LEFT JOIN states s ON stm.metadata_id = s.metadata_id AND s.attributes_id IS NOT NULL
                    LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
                    WHERE stm.entity_id IN ({placeholders})
                    """
                    
                    cursor.execute(attr_query, entity_ids)
                    for attr_row in cursor.fetchall():
                        entity_id = attr_row[0]
                        attribute_map[entity_id] = {
                            'friendly_name': attr_row[1],
                            'device_class': attr_row[2],
                            'state_class': attr_row[3]
                        }
                    
                    logger.info(f"Retrieved attributes for {len(attribute_map)} entities")
            
            # Build final metadata objects
            metadata_list = []
            for row in base_rows:
                entity_id = row[1]
                
                # Get attributes from lookup
                attrs = attribute_map.get(entity_id, {})
                friendly_name = attrs.get('friendly_name')
                device_class = attrs.get('device_class')
                state_class = attrs.get('state_class')
                
                # Apply fallback friendly name
                if not friendly_name:
                    friendly_name = entity_id.replace('sensor.', '').replace('_', ' ')
                
                # Filter out timestamp-only entities  
                if state_class == 'timestamp':
                    logger.debug(f"Skipping timestamp-only entity: {entity_id}")
                    continue
                    
                metadata = StatisticMetadata(
                    id=row[0],
                    statistic_id=entity_id,
                    source=row[2],
                    unit_of_measurement=row[3],
                    has_mean=bool(row[4]),
                    has_sum=bool(row[5]),
                    name=row[6],
                    mean_type=row[7],
                    unit_class=row[8],
                    friendly_name=friendly_name,
                    device_class=device_class,
                    state_class=state_class
                )
                metadata_list.append(metadata)
            
            logger.info(f"Final result: {len(metadata_list)} metadata records with attributes")
            return metadata_list
    
    def get_statistics_metadata_count(self) -> int:
        """Get fast count of unique entities - OPTIMIZED."""
        # Simple count of unique entities in statistics_meta
        # This returns the actual entity count (~433), not the JOIN result count (2.8M)

        with self.get_connection() as conn:
            cursor = conn.cursor()
            query = "SELECT COUNT(*) FROM statistics_meta"

            cursor.execute(query)
            count = cursor.fetchone()[0]
            logger.info(f"Total unique entities: {count}")
            return count
    
    def iter_statistics_metadata(self, batch_size: Optional[int] = None) -> Iterator[List[StatisticMetadata]]:
        """
        Iterate over statistics metadata records - OPTIMIZED with subquery.

        Returns all metadata in a single query using subquery to get latest attributes only.
        This eliminates the Cartesian product (2.8M rows -> 433 entities).

        Args:
            batch_size: Deprecated - kept for compatibility but not used (all rows returned at once)

        Yields:
            Single batch with all StatisticMetadata objects
        """
        # Check if states_meta table exists
        check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='states_meta'"

        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(check_query)
            has_states_meta = cursor.fetchone() is not None

            if has_states_meta:
                # OPTIMIZED: Subquery to get only latest attributes per entity
                # This reduces result from 2.8M rows to ~433 entities
                query = """
                SELECT sm.id, sm.statistic_id, sm.source, sm.unit_of_measurement,
                       sm.has_mean, sm.has_sum, sm.name, sm.mean_type, sm.unit_class,
                       COALESCE(JSON_EXTRACT(sa.shared_attrs, '$.friendly_name'),
                               REPLACE(REPLACE(sm.statistic_id, 'sensor.', ''), '_', ' ')) as friendly_name,
                       JSON_EXTRACT(sa.shared_attrs, '$.device_class') as device_class,
                       JSON_EXTRACT(sa.shared_attrs, '$.state_class') as state_class
                FROM statistics_meta sm
                LEFT JOIN states_meta stm ON sm.statistic_id = stm.entity_id
                LEFT JOIN (
                    SELECT s.metadata_id, s.attributes_id
                    FROM states s
                    INNER JOIN (
                        SELECT metadata_id, MAX(last_updated_ts) as max_ts
                        FROM states
                        WHERE attributes_id IS NOT NULL
                        GROUP BY metadata_id
                    ) latest_ts ON s.metadata_id = latest_ts.metadata_id
                                AND s.last_updated_ts = latest_ts.max_ts
                    WHERE s.attributes_id IS NOT NULL
                    GROUP BY s.metadata_id
                ) latest ON stm.metadata_id = latest.metadata_id
                LEFT JOIN state_attributes sa ON latest.attributes_id = sa.attributes_id
                ORDER BY sm.id
                """
                logger.info("Using optimized subquery for metadata loading (single query, ~433 entities)")
            else:
                # Fallback without states_meta
                query = """
                SELECT id, statistic_id, source, unit_of_measurement, has_mean, has_sum,
                       name, mean_type, unit_class, name, NULL, NULL
                FROM statistics_meta
                ORDER BY id
                """
                logger.warning("Using basic metadata query (no states_meta table)")

            # Execute single query - no pagination needed
            cursor.execute(query)
            rows = cursor.fetchall()

            # Convert to metadata objects
            batch = []
            for row in rows:
                # Filter out timestamp-only entities
                state_class = row[11] if len(row) > 11 else None
                if state_class == 'timestamp':
                    logger.debug(f"Skipping timestamp-only entity: {row[1]}")
                    continue

                metadata = StatisticMetadata(
                    id=row[0],
                    statistic_id=row[1],
                    source=row[2],
                    unit_of_measurement=row[3],
                    has_mean=bool(row[4]),
                    has_sum=bool(row[5]),
                    name=row[6],
                    mean_type=row[7],
                    unit_class=row[8],
                    friendly_name=row[9] if len(row) > 9 else None,
                    device_class=row[10] if len(row) > 10 else None,
                    state_class=state_class
                )
                batch.append(metadata)

            logger.info(f"Loaded {len(batch)} unique entities (optimized subquery)")
            yield batch
    
    def get_metadata_by_ids(self, metadata_ids: List[int]) -> Dict[int, StatisticMetadata]:
        """Get metadata for specific metadata IDs."""
        if not metadata_ids:
            return {}
        
        placeholders = ','.join('?' * len(metadata_ids))
        query = f"""
        SELECT id, statistic_id, source, unit_of_measurement, has_mean, has_sum,
               name, mean_type, unit_class
        FROM statistics_meta
        WHERE id IN ({placeholders})
        """
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, metadata_ids)
            
            metadata_dict = {}
            for row in cursor.fetchall():
                metadata = StatisticMetadata(
                    id=row[0],
                    statistic_id=row[1],
                    source=row[2],
                    unit_of_measurement=row[3],
                    has_mean=bool(row[4]),
                    has_sum=bool(row[5]),
                    name=row[6],
                    mean_type=row[7],
                    unit_class=row[8]
                )
                metadata_dict[metadata.id] = metadata
            
            return metadata_dict
    
    def get_statistics_count(self, table_name: str, metadata_ids: Optional[List[int]] = None) -> int:
        """Get count of statistics records in specified table."""
        if table_name not in ['statistics', 'statistics_short_term']:
            raise ValueError(f"Invalid table name: {table_name}")
        
        if not metadata_ids:
            query = f"SELECT COUNT(*) FROM {table_name}"
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                return cursor.fetchone()[0]
        
        # Handle large metadata_ids lists by batching to avoid SQLite variable limit
        batch_size = 999  # SQLite limit is ~1000 variables
        total_count = 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for i in range(0, len(metadata_ids), batch_size):
                batch = metadata_ids[i:i + batch_size]
                placeholders = ','.join('?' * len(batch))
                query = f"SELECT COUNT(*) FROM {table_name} WHERE metadata_id IN ({placeholders})"
                
                cursor.execute(query, batch)
                batch_count = cursor.fetchone()[0]
                total_count += batch_count
        
        return total_count
    
    def get_statistics_time_range(self, table_name: str) -> Tuple[float, float]:
        """Get the time range of statistics in the specified table."""
        if table_name not in ['statistics', 'statistics_short_term']:
            raise ValueError(f"Invalid table name: {table_name}")
        
        query = f"SELECT MIN(start_ts), MAX(start_ts) FROM {table_name}"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0], result[1]
    
    def iter_statistics(self, 
                       table_name: str, 
                       metadata_ids: Optional[List[int]] = None,
                       start_time: Optional[float] = None,
                       end_time: Optional[float] = None,
                       batch_size: int = 1000) -> Iterator[List[StatisticRecord]]:
        """
        Iterate over statistics records in batches.
        
        Args:
            table_name: 'statistics' or 'statistics_short_term'
            metadata_ids: Filter by specific metadata IDs
            start_time: Filter by start timestamp (inclusive)
            end_time: Filter by end timestamp (exclusive)
            batch_size: Number of records per batch
            
        Yields:
            Batches of StatisticRecord objects
        """
        if table_name not in ['statistics', 'statistics_short_term']:
            raise ValueError(f"Invalid table name: {table_name}")
        
        # Build query with filters (avoid massive IN clauses for large metadata_ids lists)
        conditions = []
        params = []
        
        # Only use IN clause for small metadata_ids lists to avoid SQLite limits
        use_sql_filter = metadata_ids and len(metadata_ids) <= 999
        if use_sql_filter:
            placeholders = ','.join('?' * len(metadata_ids))
            conditions.append(f"metadata_id IN ({placeholders})")
            params.extend(metadata_ids)
        # For large lists, we'll filter in Python after reading
        metadata_ids_set = set(metadata_ids) if metadata_ids and not use_sql_filter else None
        
        if start_time is not None:
            conditions.append("start_ts >= ?")
            params.append(start_time)
        
        if end_time is not None:
            conditions.append("start_ts < ?")
            params.append(end_time)
        
        where_clause = " AND ".join(conditions)
        if where_clause:
            where_clause = f"WHERE {where_clause}"
        
        query = f"""
        SELECT id, created_ts, metadata_id, start_ts, mean, mean_weight,
               min, max, last_reset_ts, state, sum
        FROM {table_name}
        {where_clause}
        ORDER BY metadata_id, start_ts
        LIMIT ? OFFSET ?
        """
        
        offset = 0
        with self.get_connection() as conn:
            while True:
                cursor = conn.cursor()
                cursor.execute(query, params + [batch_size, offset])
                rows = cursor.fetchall()
                
                if not rows:
                    break
                
                batch = []
                for row in rows:
                    # If we have a large metadata_ids list, filter in Python using set for O(1) lookup
                    if metadata_ids_set and row[2] not in metadata_ids_set:
                        continue
                    
                    record = StatisticRecord(
                        id=row[0],
                        created_ts=row[1],
                        metadata_id=row[2],
                        start_ts=row[3],
                        mean=row[4],
                        mean_weight=row[5],
                        min_value=row[6],
                        max_value=row[7],
                        last_reset_ts=row[8],
                        value=row[9],
                        sum_value=row[10]
                    )
                    batch.append(record)
                
                logger.debug(f"Retrieved batch of {len(batch)} records from {table_name} (offset: {offset})")
                yield batch
                
                offset += batch_size
                
                # Break if we got fewer records than requested (end of data)
                if len(rows) < batch_size:
                    break

    def iter_statistics_keyset(self,
                               table_name: str,
                               metadata_ids: Optional[List[int]] = None,
                               start_time: Optional[float] = None,
                               end_time: Optional[float] = None,
                               batch_size: int = 1000) -> Iterator[List[StatisticRecord]]:
        """
        Iterate over statistics records using KEYSET pagination - OPTIMIZED.

        Uses (metadata_id, start_ts) as cursor instead of OFFSET for constant query time.
        This is 2-4x faster than OFFSET pagination, especially for large datasets.

        Args:
            table_name: 'statistics' or 'statistics_short_term'
            metadata_ids: Filter by specific metadata IDs
            start_time: Filter by start timestamp (inclusive)
            end_time: Filter by end timestamp (exclusive)
            batch_size: Number of records per batch

        Yields:
            Batches of StatisticRecord objects
        """
        if table_name not in ['statistics', 'statistics_short_term']:
            raise ValueError(f"Invalid table name: {table_name}")

        # Build WHERE conditions
        conditions = []
        params_prefix = []

        # Handle metadata_ids filtering
        use_sql_filter = metadata_ids and len(metadata_ids) <= 999
        if use_sql_filter:
            placeholders = ','.join('?' * len(metadata_ids))
            conditions.append(f"metadata_id IN ({placeholders})")
            params_prefix.extend(metadata_ids)
        # For large lists, we'll filter in Python after reading
        metadata_ids_set = set(metadata_ids) if metadata_ids and not use_sql_filter else None

        # Time range filters
        if start_time is not None:
            conditions.append("start_ts >= ?")
            params_prefix.append(start_time)

        if end_time is not None:
            conditions.append("start_ts < ?")
            params_prefix.append(end_time)

        # Keyset cursor position
        last_metadata_id = 0
        last_start_ts = 0.0

        with self.get_connection() as conn:
            while True:
                cursor = conn.cursor()

                # Build WHERE clause with keyset condition
                where_conditions = conditions.copy()
                params = params_prefix.copy()

                if last_metadata_id > 0:
                    # Keyset pagination: WHERE (metadata_id > ? OR (metadata_id = ? AND start_ts > ?))
                    keyset_condition = "(metadata_id > ? OR (metadata_id = ? AND start_ts > ?))"
                    where_conditions.insert(0, keyset_condition)
                    params = [last_metadata_id, last_metadata_id, last_start_ts] + params

                where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

                query = f"""
                SELECT id, created_ts, metadata_id, start_ts, mean, mean_weight,
                       min, max, last_reset_ts, state, sum
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY metadata_id, start_ts
                LIMIT ?
                """

                params.append(batch_size)
                cursor.execute(query, params)
                rows = cursor.fetchall()

                if not rows:
                    break

                batch = []
                for row in rows:
                    # Python filtering for large metadata_ids lists
                    if metadata_ids_set and row[2] not in metadata_ids_set:
                        continue

                    record = StatisticRecord(
                        id=row[0],
                        created_ts=row[1],
                        metadata_id=row[2],
                        start_ts=row[3],
                        mean=row[4],
                        mean_weight=row[5],
                        min_value=row[6],
                        max_value=row[7],
                        last_reset_ts=row[8],
                        value=row[9],
                        sum_value=row[10]
                    )
                    batch.append(record)

                if batch:
                    # Update cursor position from last record
                    last_record = batch[-1]
                    last_metadata_id = last_record.metadata_id
                    last_start_ts = last_record.start_ts

                    logger.debug(f"Retrieved batch of {len(batch)} records using keyset pagination "
                               f"(cursor: metadata_id={last_metadata_id}, start_ts={last_start_ts})")
                    yield batch

                # If we got fewer records than requested, we're done
                if len(rows) < batch_size:
                    break

    def get_entity_summary(self) -> Dict[str, Any]:
        """Get a summary of entities and their statistics."""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Count by unit of measurement
            cursor.execute("""
            SELECT unit_of_measurement, COUNT(*) as count
            FROM statistics_meta
            GROUP BY unit_of_measurement
            ORDER BY count DESC
            """)
            unit_counts = dict(cursor.fetchall())
            
            # Count by source
            cursor.execute("""
            SELECT source, COUNT(*) as count
            FROM statistics_meta
            GROUP BY source
            ORDER BY count DESC
            """)
            source_counts = dict(cursor.fetchall())
            
            # Total counts
            cursor.execute("SELECT COUNT(*) FROM statistics_meta")
            total_entities = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM statistics")
            total_long_term = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM statistics_short_term")
            total_short_term = cursor.fetchone()[0]
            
            return {
                'total_entities': total_entities,
                'total_long_term_records': total_long_term,
                'total_short_term_records': total_short_term,
                'unit_distribution': unit_counts,
                'source_distribution': source_counts
            }


# Global database interface instance
db = DatabaseInterface()