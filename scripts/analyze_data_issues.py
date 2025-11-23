#!/usr/bin/env python3
"""
Analyze data quality issues in Home Assistant statistics.
"""

import sys
import os
from collections import defaultdict
import math

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import src.config as config_module
import src.database as database_module  
import src.entity_filter as entity_filter_module
import src.data_processor as data_processor_module

config = config_module.config
db = database_module.db
entity_filter = entity_filter_module.entity_filter
data_processor = data_processor_module.data_processor


def analyze_data_issues():
    """Analyze data quality issues in detail."""
    print("="*70)
    print("HOME ASSISTANT DATA QUALITY ANALYSIS")
    print("="*70)
    
    # Load and filter entities
    print("Loading and filtering entities...")
    metadata_list = db.get_statistics_metadata()
    entities, _ = entity_filter.filter_entities(metadata_list)
    print(f"Analyzing {len(entities)} filtered entities")
    
    # Create entity lookup
    entity_lookup = {e.metadata.id: e for e in entities}
    metadata_ids = [e.metadata.id for e in entities]
    
    # Analyze both tables
    for table_name in ['statistics_short_term', 'statistics']:
        print(f"\n{'='*50}")
        print(f"ANALYZING TABLE: {table_name}")
        print(f"{'='*50}")
        
        # Reset processor for each table
        data_processor.reset_metrics()
        
        # Sample analysis - process first few batches to get issue patterns
        batch_count = 0
        max_batches = 5  # Analyze first 5 batches for speed
        
        issue_by_entity = defaultdict(list)
        value_samples = defaultdict(list)
        
        print(f"Sampling first {max_batches} batches for issue analysis...")
        
        for batch in db.iter_statistics(table_name, metadata_ids, batch_size=1000):
            batch_count += 1
            
            # Process batch for issues
            for record in batch:
                entity = entity_lookup.get(record.metadata_id)
                if not entity:
                    continue
                
                entity_id = entity.metadata.statistic_id
                unit = entity.metadata.unit_of_measurement
                
                # Check each field for issues
                fields = [
                    ('value', record.value),
                    ('mean', record.mean), 
                    ('min_value', record.min_value),
                    ('max_value', record.max_value),
                    ('sum_value', record.sum_value)
                ]
                
                for field_name, value in fields:
                    if value is not None:
                        # Check for specific issues
                        if math.isnan(value):
                            issue_by_entity[entity_id].append(f"{field_name}: NaN")
                            
                        elif math.isinf(value):
                            issue_by_entity[entity_id].append(f"{field_name}: Infinite")
                            
                        elif value == -999999:
                            issue_by_entity[entity_id].append(f"{field_name}: Error sentinel (-999999)")
                            
                        elif value == 999999:
                            issue_by_entity[entity_id].append(f"{field_name}: Error sentinel (999999)")
                            
                        elif unit == "%" and (value < 0 or value > 100):
                            issue_by_entity[entity_id].append(f"{field_name}: Percentage out of range ({value}%)")
                            
                        elif unit == "°C" and (value < -50 or value > 80):
                            issue_by_entity[entity_id].append(f"{field_name}: Temperature out of range ({value}°C)")
                            
                        elif unit == "kWh" and value < 0:
                            issue_by_entity[entity_id].append(f"{field_name}: Negative energy ({value} kWh)")
                        
                        # Collect samples of unusual values
                        if abs(value) > 10000 and unit not in ["kWh", "GB", "MB"]:
                            value_samples[f"{entity_id}_{field_name}"].append(value)
            
            if batch_count >= max_batches:
                break
        
        # Report findings
        print(f"\nIssue Summary for {table_name}:")
        print(f"Analyzed {batch_count * 1000:,} records")
        
        if issue_by_entity:
            print(f"\nEntities with data quality issues: {len(issue_by_entity)}")
            
            # Group issues by type
            issue_types = defaultdict(list)
            for entity_id, issues in issue_by_entity.items():
                for issue in issues:
                    issue_type = issue.split(":")[1].strip().split(" ")[0]
                    issue_types[issue_type].append(entity_id)
            
            print("\nIssue breakdown:")
            for issue_type, entities in issue_types.items():
                print(f"  {issue_type}: {len(entities)} entities affected")
                print(f"    Sample entities: {', '.join(list(set(entities))[:3])}")
                if len(set(entities)) > 3:
                    print(f"    ... and {len(set(entities)) - 3} more")
            
            # Show worst offenders
            print(f"\nWorst offenders (most issues):")
            sorted_entities = sorted(issue_by_entity.items(), 
                                   key=lambda x: len(x[1]), reverse=True)
            
            for entity_id, issues in sorted_entities[:5]:
                entity = next(e for e in entities if e.metadata.statistic_id == entity_id)
                unit = entity.metadata.unit_of_measurement or "no unit"
                print(f"  {entity_id} ({unit}): {len(issues)} issues")
                print(f"    Examples: {'; '.join(issues[:3])}")
                if len(issues) > 3:
                    print(f"    ... and {len(issues) - 3} more")
        else:
            print("✓ No data quality issues found in sample")
    
    print(f"\n{'='*70}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*70}")
    print("This analysis sampled the first few batches of each table.")
    print("Run a full dry-run export to see complete issue statistics.")
    print("\nCommand: python scripts/export.py export --dry-run")


if __name__ == "__main__":
    try:
        analyze_data_issues()
    except Exception as e:
        print(f"Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)