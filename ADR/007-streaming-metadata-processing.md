# ADR-007: Streaming Metadata Processing

## Status
Accepted

## Context
The current metadata loading approach loads all 2.8M statistics metadata records in a single query, causing a 5-6 second delay before any processing begins. This creates poor user experience with no progress feedback and high memory usage.

Current flow:
```
Single Query (5.7s) → Load 2.8M entities → Filter in memory → Process all
```

## Problem
- **Long startup delay**: 5.7 seconds before any progress shown
- **High memory usage**: 2.8M metadata objects in RAM simultaneously  
- **Poor UX**: Users think the process is frozen during initial query
- **Fragile**: Single query failure kills entire export
- **Blocking**: No pipeline parallelism between DB reads and InfluxDB writes

## Decision

### Implement Streaming Metadata Pipeline

Replace single bulk query with batched streaming approach:

```
Streaming Query → Batch Processing → Pipeline to InfluxDB
(LIMIT/OFFSET)    (BATCH_SIZE)      (Immediate writes)
```

### Architecture Components

1. **Metadata Iterator**: 
   - Use LIMIT/OFFSET pagination through statistics_meta
   - Maintain same complex JOIN for friendly names
   - Process in configurable batch sizes (default: 5000)

2. **Pipeline Processing**:
   - Read batch → Filter entities → Convert to points → Write to InfluxDB
   - Constant memory footprint regardless of total entity count
   - Immediate progress feedback

3. **Progress Tracking**:
   - Fast initial count query for total estimation
   - Real-time progress: "Batch 150/566 (26.5%) - 45.2k entities processed"
   - Progressive statistics updates

### Implementation Strategy

**Phase 1: Fast Count Query**
```sql
SELECT COUNT(*) FROM statistics_meta sm
LEFT JOIN states_meta stm ON sm.statistic_id = stm.entity_id  
-- Same JOINs but COUNT only (fast)
```

**Phase 2: Streaming Data Query**  
```sql
SELECT sm.id, sm.statistic_id, ...friendly_name, device_class
FROM statistics_meta sm
LEFT JOIN states_meta stm ON sm.statistic_id = stm.entity_id
LEFT JOIN states s ON stm.metadata_id = s.metadata_id AND s.attributes_id IS NOT NULL
LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
LIMIT ? OFFSET ?
```

**Phase 3: Pipeline Processing**
- Each batch: metadata → filter → InfluxDB points → write
- Continuous progress reporting
- Memory usage: O(BATCH_SIZE) instead of O(total_entities)

## Consequences

### Benefits
- **Immediate feedback**: Progress visible within seconds
- **Constant memory**: RAM usage independent of database size
- **Pipeline efficiency**: DB reads, processing, and InfluxDB writes overlap
- **Resilient**: Individual batch failures don't terminate export
- **Scalable**: Works equally well with 100k or 10M entities

### Tradeoffs  
- **More complex code**: Iterator pattern vs simple list
- **Multiple queries**: LIMIT/OFFSET overhead vs single query
- **Approximate totals**: Initial count estimate may be slightly off

### Risk Mitigation
- Maintain existing filtering logic (just apply per-batch)
- Keep same friendly name extraction (per-batch JOIN)
- Preserve progress checkpoint system
- Fallback to current approach if streaming fails

## Performance Expectations

**Current**:
- 5.7s delay → Process 2.8M entities → Export
- Memory: ~500MB for metadata objects

**Expected**:  
- 0.5s initial count → Immediate processing start
- Memory: ~50MB constant (10x reduction)
- Total time: Similar or faster due to pipeline efficiency

## Implementation Notes

### Configuration
```bash
# Metadata processing batch size (default: 5000)
METADATA_BATCH_SIZE=5000

# Statistics processing batch size (unchanged)
BATCH_SIZE=5000  
```

### Database Queries
- Reuse existing complex JOIN logic for friendly names
- Add LIMIT/OFFSET pagination wrapper
- Optimize with appropriate indexes if needed

### Progress Reporting
```
Metadata Processing: Batch 150/566 (26.5%)
  ├─ Entities loaded: 750,000
  ├─ Entities filtered: 452,000 (60.3% inclusion)  
  └─ Points written: 1,250,000
```

## Changes
- 2025-11-23: Initial proposal for streaming metadata processing
- 2025-11-23: Architecture design for pipeline efficiency
- 2025-11-23: Implementation completed with streaming iterator and progress reporting

## Related ADRs
- ADR-004: Data Processing Strategy (batch processing principles)
- ADR-005: Implementation Architecture (progress tracking and resume)