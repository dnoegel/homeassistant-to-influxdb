"""
Main export orchestration for Home Assistant statistics to InfluxDB.
"""

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict

from src.config import config
from src.database import db, StatisticMetadata
from src.entity_filter import entity_filter, FilteredEntity
from src.data_processor import data_processor, ProcessingMetrics
from src.influxdb_client import get_influx_manager, WriteResult


logger = logging.getLogger(__name__)


@dataclass
class ExportCheckpoint:
    """Checkpoint data for resuming exports."""
    export_id: str
    start_time: str
    last_update: str
    total_entities: int
    entities_completed: int
    short_term_records_processed: int
    long_term_records_processed: int
    total_points_written: int
    current_table: str  # 'statistics' or 'statistics_short_term'
    completed_metadata_ids: List[int]
    failed_metadata_ids: List[int]
    export_config: Dict[str, Any]


@dataclass
class ExportMetrics:
    """Overall export metrics and statistics."""
    export_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_entities: int = 0
    entities_processed: int = 0
    entities_failed: int = 0
    short_term_records: int = 0
    long_term_records: int = 0
    total_records_processed: int = 0
    total_points_written: int = 0
    processing_time_seconds: float = 0.0
    write_time_seconds: float = 0.0
    data_quality_issues: int = 0
    data_corrections: int = 0
    
    @property
    def total_time_seconds(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return (datetime.now() - self.start_time).total_seconds()
    
    @property
    def records_per_second(self) -> float:
        if self.processing_time_seconds > 0:
            return self.total_records_processed / self.processing_time_seconds
        return 0.0
    
    @property
    def success_rate(self) -> float:
        if self.total_entities == 0:
            return 100.0
        return ((self.total_entities - self.entities_failed) / self.total_entities) * 100.0


class ExportProgress:
    """Manages progress tracking and reporting."""
    
    def __init__(self, total_entities: int):
        self.total_entities = total_entities
        self.current_entity = 0
        self.current_table = "statistics_short_term"
        self.last_report_time = time.time()
        self.report_interval = config.progress_interval
        
    def update(self, entities_processed: int, table_name: str):
        """Update progress counters."""
        self.current_entity = entities_processed
        self.current_table = table_name
        
        # Report progress at intervals
        current_time = time.time()
        if current_time - self.last_report_time >= self.report_interval:
            self.report_progress()
            self.last_report_time = current_time
    
    def report_progress(self):
        """Report current progress."""
        percentage = (self.current_entity / self.total_entities) * 100
        print(f"Progress: {self.current_entity}/{self.total_entities} entities "
              f"({percentage:.1f}%) - Processing {self.current_table}")
    
    def final_report(self, metrics: ExportMetrics):
        """Generate final progress report."""
        print(f"\nExport completed: {metrics.entities_processed}/{metrics.total_entities} entities")
        if metrics.entities_failed > 0:
            print(f"Failed entities: {metrics.entities_failed}")


class StatisticsExporter:
    """Main exporter for Home Assistant statistics."""
    
    def __init__(self):
        """Initialize the statistics exporter."""
        self.metrics = None
        self.checkpoint = None
        self.influx_manager = None
        
    def export_statistics(self, 
                         resume: bool = False,
                         dry_run: bool = False,
                         entity_filter_pattern: Optional[str] = None) -> bool:
        """
        Export Home Assistant statistics to InfluxDB.
        
        Args:
            resume: Resume from previous checkpoint
            dry_run: Validate without writing to InfluxDB
            entity_filter_pattern: Optional pattern to filter entities
            
        Returns:
            True if export completed successfully
        """
        export_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        config.setup_logging()
        
        print("="*60)
        print("HOME ASSISTANT TO INFLUXDB EXPORT")
        print("="*60)
        
        try:
            # Initialize export
            if not self._initialize_export(export_id, resume, dry_run):
                return False
            
            # Load entities
            entities = self._load_and_filter_entities(entity_filter_pattern)
            if not entities:
                print("No entities to export after filtering")
                return False
            
            # Initialize or resume from checkpoint
            if resume and self._load_checkpoint():
                print(f"Resuming export from checkpoint: {self.checkpoint.entities_completed} entities completed")
                entities = self._filter_entities_for_resume(entities)
            else:
                self._create_new_checkpoint(export_id, entities)
            
            # Setup InfluxDB
            if not dry_run and not self._setup_influxdb():
                return False
            
            # Initialize metrics and progress tracking
            self.metrics = ExportMetrics(
                export_id=export_id,
                start_time=datetime.now(),
                total_entities=len(entities)
            )
            
            progress = ExportProgress(len(entities))
            
            # Export data
            success = self._export_all_data(entities, progress, dry_run)
            
            # Finalize export
            self._finalize_export(progress, success)
            
            return success
            
        except Exception as e:
            logger.exception("Export failed with exception")
            print(f"Export failed: {e}")
            return False
    
    def _initialize_export(self, export_id: str, resume: bool, dry_run: bool) -> bool:
        """Initialize the export process."""
        print(f"Export ID: {export_id}")
        print(f"Resume mode: {resume}")
        print(f"Dry run: {dry_run}")
        
        if dry_run:
            print("⚠️  DRY RUN MODE - No data will be written to InfluxDB")
        
        # Print configuration summary
        config.print_summary()
        
        return True
    
    def _load_and_filter_entities(self, filter_pattern: Optional[str]) -> List[FilteredEntity]:
        """Load and filter entities using streaming approach."""
        
        if config.use_latest_metadata_only:
            print("\nUsing fast mode: latest metadata only per entity...")
            
            # Fast mode: get latest metadata per entity
            metadata_list = db.get_statistics_metadata_latest_only()
            print(f"Loaded {len(metadata_list):,} latest metadata records")
            
            # Apply filtering
            entities, filter_stats = entity_filter.filter_entities(metadata_list)
            
        else:
            print("\nUsing complete mode: streaming all metadata records...")
            
            # Get fast count for progress tracking
            total_metadata_count = db.get_statistics_metadata_count()
            print(f"Total metadata records: {total_metadata_count:,}")
            
            # Stream and filter metadata in batches
            entities = []
            total_loaded = 0
            batch_count = 0
            filter_stats = None
            
            print(f"Processing {total_metadata_count:,} metadata records in batches of {config.metadata_batch_size:,}")
            
            for metadata_batch in db.iter_statistics_metadata():
                batch_start_time = time.time()
                batch_count += 1
                total_loaded += len(metadata_batch)
                
                # Temporarily suppress entity filter logging
                filter_logger = logging.getLogger('src.entity_filter')
                original_level = filter_logger.level
                filter_logger.setLevel(logging.WARNING)
                
                try:
                    # Apply filtering to this batch
                    batch_entities, batch_stats = entity_filter.filter_entities(metadata_batch)
                    entities.extend(batch_entities)
                finally:
                    # Restore original log level
                    filter_logger.setLevel(original_level)
                
                # Update cumulative stats (use last batch stats as template)
                filter_stats = batch_stats
                
                # Progress reporting (matching statistics export format)
                if batch_count % config.progress_interval == 0:
                    batch_time = time.time() - batch_start_time
                    rate = len(metadata_batch) / batch_time if batch_time > 0 else 0
                    print(f"  Batch {batch_count}: {len(metadata_batch)} metadata records processed "
                          f"({rate:.0f} rec/sec, {len(batch_entities)} filtered)")
            
            print(f"\n✓ Streaming completed: {total_loaded:,} metadata records processed")
        
        print(f"✓ Total filtered entities: {len(entities):,}")
        
        # Apply additional pattern filter if specified
        if filter_pattern:
            entities = [e for e in entities if filter_pattern.lower() in e.metadata.statistic_id.lower()]
            print(f"Applied pattern filter '{filter_pattern}': {len(entities)} entities remaining")
        
        return entities
    
    def _load_checkpoint(self) -> bool:
        """Load checkpoint data for resume."""
        checkpoint_path = Path(config.checkpoint_file)
        
        if not checkpoint_path.exists():
            print("No checkpoint file found")
            return False
        
        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint_data = json.load(f)
            
            self.checkpoint = ExportCheckpoint(**checkpoint_data)
            print(f"Loaded checkpoint from {checkpoint_path}")
            return True
            
        except Exception as e:
            print(f"Failed to load checkpoint: {e}")
            return False
    
    def _create_new_checkpoint(self, export_id: str, entities: List[FilteredEntity]):
        """Create a new checkpoint for the export."""
        self.checkpoint = ExportCheckpoint(
            export_id=export_id,
            start_time=datetime.now().isoformat(),
            last_update=datetime.now().isoformat(),
            total_entities=len(entities),
            entities_completed=0,
            short_term_records_processed=0,
            long_term_records_processed=0,
            total_points_written=0,
            current_table="statistics_short_term",
            completed_metadata_ids=[],
            failed_metadata_ids=[],
            export_config={
                'batch_size': config.batch_size,
                'include_units': config.include_units,
                'influx_bucket_recent': config.influx_bucket_recent,
                'influx_bucket_historical': config.influx_bucket_historical
            }
        )
    
    def _save_checkpoint(self):
        """Save current checkpoint to disk."""
        if not self.checkpoint:
            return
        
        try:
            self.checkpoint.last_update = datetime.now().isoformat()
            checkpoint_path = Path(config.checkpoint_file)
            
            with open(checkpoint_path, 'w') as f:
                json.dump(asdict(self.checkpoint), f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def _filter_entities_for_resume(self, entities: List[FilteredEntity]) -> List[FilteredEntity]:
        """Filter entities based on checkpoint progress."""
        if not self.checkpoint:
            return entities
        
        # Remove already completed entities
        completed_ids = set(self.checkpoint.completed_metadata_ids)
        filtered_entities = [e for e in entities if e.metadata.id not in completed_ids]
        
        print(f"Resume: skipping {len(completed_ids)} already completed entities")
        return filtered_entities
    
    def _setup_influxdb(self) -> bool:
        """Setup InfluxDB connection and verify buckets."""
        print("\nSetting up InfluxDB...")
        
        try:
            self.influx_manager = get_influx_manager()
            
            # Verify buckets exist
            recent_exists = self.influx_manager.bucket_exists(config.influx_bucket_recent)
            historical_exists = self.influx_manager.bucket_exists(config.influx_bucket_historical)
            
            if not recent_exists or not historical_exists:
                print("Required buckets not found. Run setup_influx.py first.")
                return False
            
            print("✓ InfluxDB connection and buckets verified")
            return True
            
        except Exception as e:
            print(f"InfluxDB setup failed: {e}")
            return False
    
    def _export_all_data(self, entities: List[FilteredEntity], 
                        progress: ExportProgress, dry_run: bool) -> bool:
        """Export all statistics data."""
        print(f"\nStarting export of {len(entities)} entities...")
        
        # Create metadata lookup
        metadata_lookup = {e.metadata.id: e.metadata for e in entities}
        metadata_ids = [e.metadata.id for e in entities]
        
        # Export short-term data first
        if not self._export_table_data(
            "statistics_short_term", 
            entities, 
            metadata_lookup, 
            metadata_ids, 
            config.influx_bucket_recent,
            progress,
            dry_run
        ):
            return False
        
        # Then export long-term data
        if not self._export_table_data(
            "statistics", 
            entities, 
            metadata_lookup, 
            metadata_ids, 
            config.influx_bucket_historical,
            progress, 
            dry_run
        ):
            return False
        
        return True
    
    def _export_table_data(self, 
                          table_name: str,
                          entities: List[FilteredEntity],
                          metadata_lookup: Dict[int, StatisticMetadata],
                          metadata_ids: List[int],
                          bucket: str,
                          progress: ExportProgress,
                          dry_run: bool) -> bool:
        """Export data from a specific statistics table."""
        
        print(f"\nExporting {table_name} to bucket '{bucket}'...")
        
        # Get record count for progress tracking
        total_records = db.get_statistics_count(table_name, metadata_ids)
        print(f"Processing {total_records:,} records in batches of {config.batch_size}")
        
        batch_count = 0
        records_processed = 0
        
        try:
            # Process in batches
            for batch in db.iter_statistics(table_name, metadata_ids, batch_size=config.batch_size):
                batch_count += 1
                batch_start_time = time.time()
                
                # Process the batch
                processed_records, batch_metrics = data_processor.process_record_batch(
                    batch, entities, metadata_lookup
                )
                
                processing_time = time.time() - batch_start_time
                
                # Write to InfluxDB (unless dry run)
                if not dry_run and processed_records:
                    write_result = self._write_batch_to_influx(
                        processed_records, bucket
                    )
                    
                    if not write_result.success:
                        print(f"Failed to write batch {batch_count}: {write_result.errors}")
                        return False
                    
                    self.metrics.total_points_written += write_result.points_written
                
                # Update metrics
                records_processed += len(batch)
                self.metrics.total_records_processed += len(batch)
                self.metrics.processing_time_seconds += processing_time
                self.metrics.data_corrections += batch_metrics.corrected_records
                self.metrics.data_quality_issues += batch_metrics.validation_failures
                
                # Update checkpoint
                if table_name == "statistics_short_term":
                    self.checkpoint.short_term_records_processed += len(batch)
                else:
                    self.checkpoint.long_term_records_processed += len(batch)
                
                self.checkpoint.current_table = table_name
                
                # Progress reporting
                if batch_count % config.progress_interval == 0:
                    rate = len(batch) / processing_time if processing_time > 0 else 0
                    print(f"  Batch {batch_count}: {len(batch)} records processed "
                          f"({rate:.0f} rec/sec, {batch_metrics.success_rate:.1f}% valid)")
                    
                    # Save checkpoint periodically
                    self._save_checkpoint()
            
            # Table completion summary
            print(f"✓ {table_name} export completed: {records_processed:,} records processed")
            
            if table_name == "statistics_short_term":
                self.metrics.short_term_records = records_processed
            else:
                self.metrics.long_term_records = records_processed
            
            return True
            
        except Exception as e:
            logger.exception(f"Failed to export {table_name}")
            print(f"Export of {table_name} failed: {e}")
            return False
    
    def _write_batch_to_influx(self, processed_records: List[Tuple[FilteredEntity, Any]], 
                              bucket: str) -> WriteResult:
        """Write a batch of processed records to InfluxDB."""
        write_start = time.time()
        
        # Create points
        points = []
        for entity, record in processed_records:
            point = self.influx_manager.create_point(entity, record)
            if point:
                points.append(point)
        
        # Write to InfluxDB
        result = self.influx_manager.write_points(points, bucket)
        
        self.metrics.write_time_seconds += time.time() - write_start
        
        return result
    
    def _finalize_export(self, progress: ExportProgress, success: bool):
        """Finalize the export process."""
        self.metrics.end_time = datetime.now()
        self.metrics.entities_processed = len(self.checkpoint.completed_metadata_ids) if self.checkpoint else 0
        
        # Final reports
        progress.final_report(self.metrics)
        data_processor.print_processing_stats()
        self._print_export_summary()
        
        # Clean up checkpoint on success
        if success and config.resume_enabled:
            try:
                Path(config.checkpoint_file).unlink(missing_ok=True)
                print("✓ Checkpoint file cleaned up")
            except Exception as e:
                logger.warning(f"Failed to clean up checkpoint: {e}")
        
        # Close InfluxDB connection
        if self.influx_manager:
            self.influx_manager.close()
    
    def _print_export_summary(self):
        """Print detailed export summary."""
        print("\n" + "="*60)
        print("EXPORT SUMMARY")
        print("="*60)
        print(f"Export ID: {self.metrics.export_id}")
        print(f"Total time: {self.metrics.total_time_seconds:.1f} seconds")
        print(f"Entities processed: {self.metrics.entities_processed}")
        print(f"Records processed: {self.metrics.total_records_processed:,}")
        print(f"  Short-term: {self.metrics.short_term_records:,}")
        print(f"  Long-term: {self.metrics.long_term_records:,}")
        print(f"Points written to InfluxDB: {self.metrics.total_points_written:,}")
        print(f"Processing rate: {self.metrics.records_per_second:.0f} records/sec")
        print(f"Success rate: {self.metrics.success_rate:.1f}%")
        
        if self.metrics.data_quality_issues > 0:
            print(f"\nData quality:")
            print(f"  Issues detected: {self.metrics.data_quality_issues:,}")
            print(f"  Auto-corrections: {self.metrics.data_corrections:,}")


# Global exporter instance
exporter = StatisticsExporter()