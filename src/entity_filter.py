"""
Entity filtering logic for selecting relevant statistics for InfluxDB export.
"""

import logging
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass
from enum import Enum

from src.database import StatisticMetadata
from src.config import config


logger = logging.getLogger(__name__)


class SensorCategory(Enum):
    """Categories of sensors for different aggregation strategies."""
    ENERGY = "energy"           # kWh - cumulative energy consumption
    POWER = "power"             # W - instantaneous power
    TEMPERATURE = "temperature" # °C - environmental temperature
    PERCENTAGE = "percentage"   # % - ratios, battery levels, etc.
    ELECTRICAL = "electrical"   # A, V - current and voltage
    NETWORK = "network"         # kB/s, GB - network usage
    PRESSURE = "pressure"       # hPa, bar - atmospheric pressure
    SPECIAL = "special"         # Special integrations (e.g., Tibber)
    EXCLUDED = "excluded"       # Filtered out entities


@dataclass
class FilteredEntity:
    """Represents a filtered entity with its category and aggregation strategy."""
    metadata: StatisticMetadata
    category: SensorCategory
    aggregation_method: str
    aggregation_window: str
    reason: str  # Why this entity was included/excluded


class EntityFilter:
    """Filter entities for InfluxDB export based on relevance and type."""
    
    def __init__(self):
        """Initialize entity filter with configuration."""
        self.include_units = set(config.include_units)
        self.include_sources = set(config.include_sources)
        self.exclude_patterns = config.exclude_patterns
        self._setup_aggregation_rules()
    
    def _setup_aggregation_rules(self):
        """Define aggregation rules for different sensor categories."""
        self.aggregation_rules = {
            SensorCategory.ENERGY: {
                'method': 'last',  # Preserve cumulative values
                'window': '1h',
                'description': 'Energy consumption (cumulative)'
            },
            SensorCategory.POWER: {
                'method': 'mean',  # Average power over time
                'window': '1h',
                'description': 'Instantaneous power'
            },
            SensorCategory.TEMPERATURE: {
                'method': 'mean',  # Average temperature
                'window': '1h',
                'description': 'Environmental temperature'
            },
            SensorCategory.PERCENTAGE: {
                'method': 'mean',  # Average percentage
                'window': '1h',
                'description': 'Percentage values'
            },
            SensorCategory.ELECTRICAL: {
                'method': 'mean',  # Average electrical values
                'window': '1h',
                'description': 'Current and voltage'
            },
            SensorCategory.NETWORK: {
                'method': 'mean',  # Average network usage
                'window': '1h',
                'description': 'Network bandwidth and data'
            },
            SensorCategory.PRESSURE: {
                'method': 'mean',  # Average pressure
                'window': '1h',
                'description': 'Atmospheric pressure'
            },
            SensorCategory.SPECIAL: {
                'method': 'last',  # Preserve special integration data
                'window': '1h',
                'description': 'Special integration data'
            }
        }
    
    def categorize_entity(self, metadata: StatisticMetadata) -> SensorCategory:
        """Determine the category of an entity based on its metadata."""
        unit = metadata.unit_of_measurement
        source = metadata.source
        entity_id = metadata.statistic_id.lower()
        
        # Check for special sources first
        if source in self.include_sources:
            return SensorCategory.SPECIAL
        
        # Check exclude patterns
        for pattern in self.exclude_patterns:
            pattern_clean = pattern.replace('%', '')
            if pattern_clean in entity_id:
                return SensorCategory.EXCLUDED
        
        # Categorize by unit of measurement
        if unit == 'kWh':
            return SensorCategory.ENERGY
        elif unit == 'W':
            return SensorCategory.POWER
        elif unit in ['°C', '°F']:
            return SensorCategory.TEMPERATURE
        elif unit == '%':
            return SensorCategory.PERCENTAGE
        elif unit in ['A', 'V']:
            return SensorCategory.ELECTRICAL
        elif unit in ['kB/s', 'MB/s', 'GB', 'MB']:
            return SensorCategory.NETWORK
        elif unit in ['hPa', 'bar', 'mbar']:
            return SensorCategory.PRESSURE
        elif unit in self.include_units:
            # Other explicitly included units - categorize as special
            return SensorCategory.SPECIAL
        else:
            return SensorCategory.EXCLUDED
    
    def _should_include_entity(self, metadata: StatisticMetadata) -> Tuple[bool, str]:
        """
        Determine if an entity should be included in the export.
        
        Returns:
            Tuple of (should_include, reason)
        """
        # Check domain filtering first
        entity_parts = metadata.statistic_id.split('.')
        if len(entity_parts) < 2:
            return False, "Invalid entity ID format"
        
        domain = entity_parts[0]
        if domain not in config.include_domains:
            return False, f"Domain '{domain}' not in include list"
        
        category = self.categorize_entity(metadata)
        
        if category == SensorCategory.EXCLUDED:
            return False, "Excluded by filter patterns or unsupported unit"
        
        # Additional quality checks
        entity_id = metadata.statistic_id.lower()
        
        # Skip entities that are clearly status indicators
        status_indicators = ['availability', 'status', 'connected', 'online', 'signal', 'rssi']
        for indicator in status_indicators:
            if indicator in entity_id:
                return False, f"Status indicator: {indicator}"
        
        # Only skip entities that clearly have no time-series value
        # (entities with supported units/domains are valuable even without has_mean/has_sum flags)
        if metadata.unit_of_measurement is None and category == SensorCategory.EXCLUDED:
            return False, "No unit and no supported category"
        
        return True, f"Included as {category.value}"
    
    def filter_entities(self, metadata_list: List[StatisticMetadata]) -> Tuple[List[FilteredEntity], Dict[str, int]]:
        """
        Filter entities for InfluxDB export.
        
        Args:
            metadata_list: List of all entity metadata
            
        Returns:
            Tuple of (filtered_entities, summary_stats)
        """
        filtered_entities = []
        category_counts = {category: 0 for category in SensorCategory}
        
        for metadata in metadata_list:
            should_include, reason = self._should_include_entity(metadata)
            
            if should_include:
                category = self.categorize_entity(metadata)
                aggregation_rule = self.aggregation_rules[category]
                
                filtered_entity = FilteredEntity(
                    metadata=metadata,
                    category=category,
                    aggregation_method=aggregation_rule['method'],
                    aggregation_window=aggregation_rule['window'],
                    reason=reason
                )
                filtered_entities.append(filtered_entity)
                category_counts[category] += 1
            else:
                category_counts[SensorCategory.EXCLUDED] += 1
                logger.debug(f"Excluded {metadata.statistic_id}: {reason}")
        
        # Create summary statistics
        summary_stats = {
            'total_entities': len(metadata_list),
            'included_entities': len(filtered_entities),
            'excluded_entities': category_counts[SensorCategory.EXCLUDED],
            'inclusion_rate': len(filtered_entities) / len(metadata_list) * 100,
            'category_breakdown': {
                category.value: count 
                for category, count in category_counts.items() 
                if category != SensorCategory.EXCLUDED
            }
        }
        
        logger.info(f"Entity filtering complete:")
        logger.info(f"  Total entities: {summary_stats['total_entities']}")
        logger.info(f"  Included: {summary_stats['included_entities']}")
        logger.info(f"  Excluded: {summary_stats['excluded_entities']}")
        logger.info(f"  Inclusion rate: {summary_stats['inclusion_rate']:.1f}%")
        
        for category, count in summary_stats['category_breakdown'].items():
            if count > 0:
                logger.info(f"  {category.title()}: {count}")
        
        return filtered_entities, summary_stats
    
    def get_metadata_ids(self, filtered_entities: List[FilteredEntity]) -> List[int]:
        """Extract metadata IDs from filtered entities."""
        return [entity.metadata.id for entity in filtered_entities]
    
    def group_by_category(self, filtered_entities: List[FilteredEntity]) -> Dict[SensorCategory, List[FilteredEntity]]:
        """Group filtered entities by their category."""
        groups = {category: [] for category in SensorCategory if category != SensorCategory.EXCLUDED}
        
        for entity in filtered_entities:
            groups[entity.category].append(entity)
        
        return groups
    
    def print_filter_summary(self, filtered_entities: List[FilteredEntity], summary_stats: Dict):
        """Print a detailed summary of the filtering results."""
        print("\n" + "="*60)
        print("ENTITY FILTERING SUMMARY")
        print("="*60)
        
        print(f"Total entities processed: {summary_stats['total_entities']}")
        print(f"Entities selected for export: {summary_stats['included_entities']}")
        print(f"Entities excluded: {summary_stats['excluded_entities']}")
        print(f"Selection rate: {summary_stats['inclusion_rate']:.1f}%")
        
        print("\nBreakdown by category:")
        for category, count in summary_stats['category_breakdown'].items():
            if count > 0:
                print(f"  {category.title():<15}: {count:>4} entities")
        
        print("\nAggregation strategies:")
        for category in SensorCategory:
            if category == SensorCategory.EXCLUDED:
                continue
            entities_in_category = [e for e in filtered_entities if e.category == category]
            if entities_in_category:
                rule = self.aggregation_rules[category]
                print(f"  {category.value.title():<15}: {rule['method']} over {rule['window']}")
        
        print("\nSample included entities:")
        by_category = self.group_by_category(filtered_entities)
        for category, entities in by_category.items():
            if entities:
                print(f"\n  {category.value.title()}:")
                for entity in entities[:3]:  # Show first 3 entities per category
                    unit = entity.metadata.unit_of_measurement or "no unit"
                    print(f"    {entity.metadata.statistic_id} ({unit})")
                if len(entities) > 3:
                    print(f"    ... and {len(entities) - 3} more")


# Global entity filter instance
entity_filter = EntityFilter()