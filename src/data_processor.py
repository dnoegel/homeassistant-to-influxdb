"""
Data processing pipeline for Home Assistant statistics export to InfluxDB.
"""

import logging
import math
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple, Set, Any
from dataclasses import dataclass

from src.config import config
from src.database import StatisticRecord, StatisticMetadata
from src.entity_filter import FilteredEntity, SensorCategory


logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of data quality validation."""
    is_valid: bool
    issues: List[str]
    corrected_value: Optional[float] = None


@dataclass
class ProcessingMetrics:
    """Metrics for data processing operations."""
    total_records: int = 0
    valid_records: int = 0
    skipped_records: int = 0
    corrected_records: int = 0
    validation_failures: int = 0
    processing_time_seconds: float = 0.0
    issue_details: Dict[str, int] = None
    
    def __post_init__(self):
        if self.issue_details is None:
            self.issue_details = {}
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_records == 0:
            return 100.0
        return (self.valid_records / self.total_records) * 100.0


class DataQualityValidator:
    """Validates data quality for Home Assistant statistics."""
    
    def __init__(self):
        """Initialize data quality validator."""
        self.quality_rules = config.quality_rules
        self.validation_cache = {}  # Cache validation rules by unit
        self._setup_validation_rules()
    
    def _setup_validation_rules(self):
        """Set up validation rules for different measurement types."""
        # Extended quality rules beyond config
        self.extended_rules = {
            **self.quality_rules,
            'kB/s': {'min': 0, 'max': 1000000},      # Network speed
            'MB/s': {'min': 0, 'max': 1000},         # Network speed
            'GB': {'min': 0, 'max': 10000},          # Data usage
            'MB': {'min': 0, 'max': 10000000},       # Data usage
            'hPa': {'min': 800, 'max': 1200},        # Atmospheric pressure
            'bar': {'min': 0.8, 'max': 1.2},         # Pressure
        }
        
        # Common invalid values to detect
        self.invalid_patterns = {
            'nan_values': [float('nan')],
            'inf_values': [float('inf'), float('-inf')],
            'extreme_negatives': -999999,  # Often used as "unknown" value
            'extreme_positives': 999999,   # Often used as "error" value
        }
    
    def validate_value(self, value: float, unit: Optional[str], 
                      entity_category: SensorCategory) -> ValidationResult:
        """
        Validate a single measurement value.
        
        Args:
            value: The value to validate
            unit: Unit of measurement
            entity_category: Category of the sensor
            
        Returns:
            ValidationResult with validation status and any corrections
        """
        issues = []
        
        # Basic NaN/Inf checks
        if math.isnan(value):
            return ValidationResult(False, ["Value is NaN"])
        
        if math.isinf(value):
            return ValidationResult(False, ["Value is infinite"])
        
        # Check for common invalid patterns
        if value == self.invalid_patterns['extreme_negatives']:
            return ValidationResult(False, ["Value appears to be error sentinel (-999999)"])
        
        if value == self.invalid_patterns['extreme_positives']:
            return ValidationResult(False, ["Value appears to be error sentinel (999999)"])
        
        # Unit-specific validation
        if unit and unit in self.extended_rules:
            rules = self.extended_rules[unit]
            
            if rules.get('min') is not None and value < rules['min']:
                issues.append(f"Value {value} below minimum {rules['min']} for unit {unit}")
            
            if rules.get('max') is not None and value > rules['max']:
                issues.append(f"Value {value} above maximum {rules['max']} for unit {unit}")
        
        # Category-specific validation
        category_issues = self._validate_by_category(value, entity_category, unit)
        issues.extend(category_issues)
        
        # Special corrections for common issues
        corrected_value = self._apply_corrections(value, unit, entity_category)
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            issues=issues,
            corrected_value=corrected_value if corrected_value != value else None
        )
    
    def _validate_by_category(self, value: float, category: SensorCategory, 
                            unit: Optional[str]) -> List[str]:
        """Apply category-specific validation rules."""
        issues = []
        
        if category == SensorCategory.ENERGY:
            # Energy should always be positive and reasonable
            if value < 0:
                issues.append("Energy value cannot be negative")
            if value > 1000000:  # 1 million kWh seems excessive for home use
                issues.append("Energy value seems unrealistically high")
        
        elif category == SensorCategory.POWER:
            # Power can be negative (solar generation) but within reason
            if abs(value) > 100000:  # 100kW seems excessive for home
                issues.append("Power value seems unrealistically high")
        
        elif category == SensorCategory.PERCENTAGE:
            # Percentages should be 0-100, but allow some flexibility
            if value < -5 or value > 105:
                issues.append("Percentage value outside reasonable range")
        
        elif category == SensorCategory.TEMPERATURE:
            # Temperature checks are already in quality_rules, but add extremes
            if value < -100 or value > 200:
                issues.append("Temperature value outside physically reasonable range")
        
        return issues
    
    def _apply_corrections(self, value: float, unit: Optional[str], 
                          category: SensorCategory) -> float:
        """Apply automatic corrections for common data issues."""
        # Percentage normalization
        if category == SensorCategory.PERCENTAGE:
            if unit == '%':
                # Clamp percentages to reasonable range
                if value < 0:
                    return 0.0
                elif value > 100:
                    return 100.0
        
        # Energy accumulation correction (negative resets to 0)
        if category == SensorCategory.ENERGY and value < 0:
            return 0.0
        
        return value


class DataProcessor:
    """Processes Home Assistant statistics for InfluxDB export."""
    
    def __init__(self):
        """Initialize data processor."""
        self.validator = DataQualityValidator()
        self.metrics = ProcessingMetrics()
        self._processed_timestamps = set()  # Track for deduplication
        self._issue_tracking = {}  # Track detailed issues by type
    
    def process_record_batch(self, 
                           records: List[StatisticRecord],
                           entities: List[FilteredEntity],
                           metadata_lookup: Dict[int, StatisticMetadata]) -> Tuple[List[Tuple[FilteredEntity, StatisticRecord]], ProcessingMetrics]:
        """
        Process a batch of statistic records.
        
        Args:
            records: List of raw statistic records
            entities: List of filtered entities  
            metadata_lookup: Metadata lookup by ID
            
        Returns:
            Tuple of (processed_records, batch_metrics)
        """
        import time
        start_time = time.time()
        
        batch_metrics = ProcessingMetrics()
        processed_records = []
        
        # Create entity lookup for faster access
        entity_lookup = {entity.metadata.id: entity for entity in entities}
        
        for record in records:
            batch_metrics.total_records += 1
            
            # Skip records for entities not in our filtered list
            entity = entity_lookup.get(record.metadata_id)
            if not entity:
                batch_metrics.skipped_records += 1
                continue
            
            # Process the record
            processing_result = self._process_single_record(record, entity)
            
            if processing_result == "valid":
                processed_records.append((entity, record))
                batch_metrics.valid_records += 1
            elif processing_result == "corrected":
                processed_records.append((entity, record))
                batch_metrics.valid_records += 1
                batch_metrics.corrected_records += 1
            else:  # "invalid"
                batch_metrics.validation_failures += 1
                batch_metrics.skipped_records += 1
                logger.debug(f"Skipped invalid record for {entity.metadata.statistic_id}")
        
        batch_metrics.processing_time_seconds = time.time() - start_time
        
        # Update global metrics
        self._update_global_metrics(batch_metrics)
        
        return processed_records, batch_metrics
    
    def _process_single_record(self, record: StatisticRecord, entity: FilteredEntity) -> str:
        """
        Process a single statistic record.
        
        Returns:
            "valid", "corrected", or "invalid"
        """
        # Check for duplicate timestamps (basic deduplication)
        timestamp_key = (entity.metadata.id, record.start_ts)
        if timestamp_key in self._processed_timestamps:
            logger.debug(f"Duplicate timestamp detected for {entity.metadata.statistic_id}")
            return "invalid"
        
        self._processed_timestamps.add(timestamp_key)
        
        # Validate timestamp is reasonable
        if not self._is_valid_timestamp(record.start_ts):
            logger.debug(f"Invalid timestamp {record.start_ts} for {entity.metadata.statistic_id}")
            return "invalid"
        
        # Validate each numeric field that exists
        corrections_made = False
        issues_found = []
        
        # Validate and potentially correct value field
        if record.value is not None:
            validation = self.validator.validate_value(
                record.value, 
                entity.metadata.unit_of_measurement,
                entity.category
            )
            
            if not validation.is_valid:
                issues_found.extend(validation.issues)
                # Try to use corrected value if available
                if validation.corrected_value is not None:
                    record.value = validation.corrected_value
                    corrections_made = True
                    logger.debug(f"Corrected value value for {entity.metadata.statistic_id}: "
                               f"{record.value} -> {validation.corrected_value}")
                else:
                    # Cannot correct, invalidate the field
                    record.value = None
        
        # Validate other fields similarly
        for field_name, field_value in [
            ('mean', record.mean),
            ('min_value', record.min_value), 
            ('max_value', record.max_value),
            ('sum_value', record.sum_value)
        ]:
            if field_value is not None:
                validation = self.validator.validate_value(
                    field_value,
                    entity.metadata.unit_of_measurement, 
                    entity.category
                )
                
                if not validation.is_valid:
                    issues_found.extend(validation.issues)
                    if validation.corrected_value is not None:
                        setattr(record, field_name, validation.corrected_value)
                        corrections_made = True
                    else:
                        setattr(record, field_name, None)
        
        # Track issues for reporting
        if issues_found:
            self._track_issues(issues_found, entity.metadata.statistic_id)
        
        # Check if record has any valid data left
        if (record.value is None and record.mean is None and 
            record.min_value is None and record.max_value is None and 
            record.sum_value is None):
            logger.debug(f"No valid fields remaining for {entity.metadata.statistic_id}")
            return "invalid"
        
        return "corrected" if corrections_made else "valid"
    
    def _is_valid_timestamp(self, timestamp: float) -> bool:
        """Check if a timestamp is reasonable."""
        try:
            # Check if timestamp is within reasonable bounds
            # Should be between 2020 and 2030 for this use case
            min_timestamp = datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp()
            max_timestamp = datetime(2030, 1, 1, tzinfo=timezone.utc).timestamp()
            
            return min_timestamp <= timestamp <= max_timestamp
        except (ValueError, OSError):
            return False
    
    def _track_issues(self, issues: List[str], entity_id: str):
        """Track issues for detailed reporting."""
        for issue in issues:
            # Categorize the issue type
            if "NaN" in issue:
                category = "NaN values"
            elif "infinite" in issue:
                category = "Infinite values"
            elif "below minimum" in issue or "above maximum" in issue:
                category = "Out of range values"
            elif "negative" in issue:
                category = "Negative values (invalid for type)"
            elif "error sentinel" in issue:
                category = "Error sentinel values"
            else:
                category = "Other validation issues"
            
            if category not in self._issue_tracking:
                self._issue_tracking[category] = {"count": 0, "entities": set()}
            
            self._issue_tracking[category]["count"] += 1
            self._issue_tracking[category]["entities"].add(entity_id)
    
    def _update_global_metrics(self, batch_metrics: ProcessingMetrics):
        """Update global processing metrics."""
        self.metrics.total_records += batch_metrics.total_records
        self.metrics.valid_records += batch_metrics.valid_records
        self.metrics.skipped_records += batch_metrics.skipped_records
        self.metrics.corrected_records += batch_metrics.corrected_records
        self.metrics.validation_failures += batch_metrics.validation_failures
        self.metrics.processing_time_seconds += batch_metrics.processing_time_seconds
        
        # Update issue details
        self.metrics.issue_details = {
            category: data["count"] 
            for category, data in self._issue_tracking.items()
        }
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Get a summary of processing metrics."""
        return {
            'total_records_processed': self.metrics.total_records,
            'valid_records': self.metrics.valid_records,
            'corrected_records': self.metrics.corrected_records,
            'skipped_records': self.metrics.skipped_records,
            'validation_failures': self.metrics.validation_failures,
            'success_rate_percent': self.metrics.success_rate,
            'total_processing_time_seconds': self.metrics.processing_time_seconds,
            'records_per_second': (
                self.metrics.total_records / self.metrics.processing_time_seconds
                if self.metrics.processing_time_seconds > 0 else 0
            )
        }
    
    def reset_metrics(self):
        """Reset processing metrics."""
        self.metrics = ProcessingMetrics()
        self._processed_timestamps.clear()
        self._issue_tracking.clear()
    
    def print_processing_stats(self):
        """Print detailed processing statistics."""
        summary = self.get_processing_summary()
        
        print("\n" + "="*50)
        print("DATA PROCESSING SUMMARY")
        print("="*50)
        print(f"Total records processed: {summary['total_records_processed']:,}")
        print(f"Valid records: {summary['valid_records']:,}")
        print(f"Corrected records: {summary['corrected_records']:,}")
        print(f"Skipped/Invalid records: {summary['skipped_records']:,}")
        print(f"Success rate: {summary['success_rate_percent']:.1f}%")
        print(f"Processing time: {summary['total_processing_time_seconds']:.2f}s")
        print(f"Processing rate: {summary['records_per_second']:.0f} records/sec")
        
        if summary['validation_failures'] > 0:
            print(f"\nData Quality Issues:")
            print(f"  Validation failures: {summary['validation_failures']:,}")
            print(f"  Auto-corrections: {summary['corrected_records']:,}")
            
            # Show detailed issue breakdown
            if self._issue_tracking:
                print(f"\nIssue Breakdown:")
                for category, data in self._issue_tracking.items():
                    entity_count = len(data["entities"])
                    print(f"  {category}: {data['count']:,} issues across {entity_count} entities")
                    
                    # Show sample entities for each issue type
                    sample_entities = list(data["entities"])[:3]
                    if sample_entities:
                        print(f"    Sample entities: {', '.join(sample_entities)}")
                        if len(data["entities"]) > 3:
                            print(f"    ... and {len(data['entities']) - 3} more")


# Global data processor instance
data_processor = DataProcessor()