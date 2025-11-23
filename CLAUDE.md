# Claude Code Memory - Home Assistant Statistics Exporter

## Project Overview
Complete Home Assistant to InfluxDB 2.x statistics migration tool with intelligent entity filtering, data quality validation, and dual-bucket architecture.

## Key Implementation Decisions (from Session)

### 1. Data Schema Evolution
**FROM**: Single measurement "homeassistant" with category-based filtering
**TO**: Unit-based measurements (°C, kWh, W, etc.) for HA compatibility

- **Rationale**: Home Assistant's native InfluxDB integration uses unit_of_measurement as measurement name
- **Impact**: Enables unified querying of historical + live data
- **Flux Tasks**: Completely rewritten to filter on measurement units instead of categories

### 2. Statistical Fields Strategy
**DECISION**: Export only `value` field for consistency with HA native integration
**REASONING**: 
- HA's native InfluxDB integration uses only `value` field
- Unified schema enables seamless querying of historical + live data
- Eliminates redundancy (mean=value creates duplicate data)
- Flux aggregation tasks work on consistent field structure
- Reduced storage overhead (~75% less field data)

### 3. Domain Filtering Enhancement
**NEW**: `INCLUDE_DOMAINS` config (sensor,counter,weather,climate,utility_meter)
- Domain checked before unit/category filtering
- Counter domain gets special `max` aggregation (cumulative nature)
- More predictable than pure unit-based filtering

### 4. Entity Metadata Enhancement ⚠️ CRITICAL UPDATE
**ADDED**: 
- `friendly_name` as InfluxDB **TAG** (not field) - for efficient filtering
- `device_class` as InfluxDB tag  
- `state_class = 'timestamp'` automatic exclusion
- Enhanced database queries with states_meta join

**IMPORTANT**: `friendly_name` stored as TAG for efficient querying and filtering by device names.

### 5. Flux Aggregation Strategy
**OLD**: Category-based (`r["category"] == "energy"`)
**NEW**: Unit-based (`r["_measurement"] == "kWh"`)

**Aggregation Rules**:
- Energy (kWh, Wh): `last` (preserve cumulative)
- Power (W, kW): `mean` (average consumption)
- Temperature (°C, °F): `mean` (environmental trends) 
- Counters: `max` (cumulative increments)
- Rates (kB/s): `mean`, Data (GB): `last`

## Configuration Changes

### New Environment Variables
```bash
INCLUDE_DOMAINS=sensor,counter,weather,climate,utility_meter
METADATA_BATCH_SIZE=5000  # Streaming batch size for metadata processing
```

### Updated Units List
```bash
INCLUDE_UNITS=kWh,W,°C,°F,kB/s,GB,MB,A,V,hPa,bar,mbar,lux,ppm,dB,rpm
```

## Migration Process

### Breaking Change Migration
1. **Cleanup**: `python scripts/cleanup_old_data.py` - Remove old _measurement=homeassistant data
2. **Setup**: `python scripts/setup_influx.py` - Create new unit-based Flux tasks  
3. **Export**: `python scripts/export.py export` - Import with new schema

### Data Compatibility
- Historical export data: Uses new unit-based measurements
- Live HA integration: Compatible with same measurement naming
- Unified Flux aggregation: Works across both data sources

## Entity Filtering Logic (Final)
1. **Domain check**: Must be in INCLUDE_DOMAINS
2. **State class check**: Exclude timestamp-only entities
3. **Status pattern check**: Exclude availability/status indicators
4. **Unit/category check**: Must have meaningful time-series data
5. **Result**: ~80-85% inclusion rate

## File Structure
```
hastats/
├── src/
│   ├── database.py          # Enhanced with state_attributes
│   ├── entity_filter.py     # Domain + unit filtering
│   ├── influxdb_client.py   # Unit-based measurements
│   └── config.py           # INCLUDE_DOMAINS added
├── scripts/
│   ├── setup_influx.py     # Unit-based Flux tasks
│   ├── cleanup_old_data.py # Migration helper
│   └── export.py           # Enhanced analyze-entities
└── config/
    └── influx_tasks.flux   # Unit-based aggregation
```

## Key Learnings

### Streaming Metadata Architecture (ADR-007 Implementation)
- **Problem solved**: 5.7-second startup delay → immediate progress feedback
- **Memory efficiency**: Constant O(batch_size) usage regardless of 2.8M total records
- **OFFSET vs Cursor**: OFFSET pagination works better for complex JOINs with Cartesian products
- **Data reality**: 433 unique entities × ~6,500 metadata records each = 2.8M total records to process
- **Progress UX**: Clean batch reporting matching statistics export format

### InfluxDB Schema Design
- Tags for indexing: entity_id, domain, category, unit, source, friendly_name, device_class
- Fields for data: value (only - matches HA native schema)
- Measurement name should match data semantic meaning (units)
- **Schema consistency**: Single `value` field matches HA's native integration

### Home Assistant Database Structure - COMPLEX JOIN PATH
- `statistics_meta`: Core entity metadata (but name field often NULL)
- `states_meta`: Entity registry (metadata_id, entity_id mapping)
- `states`: Current/historical states (empty entity_id, use metadata_id)
- `state_attributes`: JSON attributes with friendly_name, device_class

**WORKING JOIN PATH**:
```sql
statistics_meta → states_meta → states(via metadata_id) → state_attributes
```

**FAILED APPROACHES**:
- Direct states join (entity_id empty CHAR(0))
- states_meta only (no attributes column)
- Simple statistics_meta.name (often NULL)

### Database Query Evolution
**Final Working Query**:
```sql
SELECT sm.*, 
       COALESCE(JSON_EXTRACT(sa.shared_attrs, '$.friendly_name'), 
               REPLACE(REPLACE(sm.statistic_id, 'sensor.', ''), '_', ' ')) as friendly_name,
       JSON_EXTRACT(sa.shared_attrs, '$.device_class') as device_class
FROM statistics_meta sm
LEFT JOIN states_meta stm ON sm.statistic_id = stm.entity_id  
LEFT JOIN states s ON stm.metadata_id = s.metadata_id AND s.attributes_id IS NOT NULL
LEFT JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
```

**Results**: Successfully extracts real friendly names like:
- `sensor.2nd_dhw_temp` → `'2nd DHW temp.'`
- `sensor.2nd_dhw_temp_2` → `'ESPAltherma 2nd DHW temp.'`

### Source Tag Strategy
- **Migration data**: `source="migration"` 
- **Live HA data**: `source="recorder"` (or HA default)
- **Flux aggregation**: Ignores source, aggregates all data by unit
- **Cleanup capability**: Filter by source for targeted deletion

### Cleanup Script Architecture
**Two-Mode Design**:
- `--migrated`: Delete only `source="migration"` data (preserve infrastructure) 
- `--all`: Delete buckets + InfluxDB tasks (complete reset)

### Memory Management & Streaming Architecture (ADR-007) ✅
- **Metadata streaming**: OFFSET pagination for 2.8M metadata records  
- **Batch processing**: 5000 metadata records per batch (configurable via METADATA_BATCH_SIZE)
- **Statistics processing**: 1000 records per batch
- **Streaming design**: Iterator pattern, constant memory usage O(batch_size)
- **Database connections**: Context managers for proper cleanup
- **Progress feedback**: Immediate streaming progress vs 5.7s startup delay

**Key Metrics**:
- Memory usage: ~50MB constant vs ~500MB bulk loading (10x reduction)
- Processing rate: 20K-40K records/sec sustained
- Total processing: 2.8M metadata records → ~450K filtered entities
- Architecture: Fast count query + OFFSET streaming + real-time progress

### SQLite Variable Limit Fixes (CRITICAL)
**Problem**: Export failed with "too many SQL variables" error when processing 1.8M entities
**Root cause**: SQLite has ~1000 parameter limit, but code tried to pass 1.8M metadata_ids

**Fixes Applied**:
1. **get_statistics_count()**: Batches metadata_ids into chunks of 999, sums results
2. **iter_statistics()**: Uses SQL IN clause for small lists (≤999), Python filtering for large lists

**Key Implementation**:
```python
# Small lists: efficient SQL WHERE metadata_id IN (...)
# Large lists: read all records, filter in Python with set lookup O(1)
use_sql_filter = metadata_ids and len(metadata_ids) <= 999
metadata_ids_set = set(metadata_ids) if metadata_ids and not use_sql_filter else None
```

### Entity Filtering Return Format (CRITICAL BUG FIX)
**Problem**: `filter_entities()` returns tuple `(entities, stats)` but code expected just entities
**Impact**: Caused `'list' object has no attribute 'category'` errors in export

**Fixed Understanding**:
```python
# WRONG usage:
filtered_entities = entity_filter.filter_entities(metadata_list)

# CORRECT usage:  
filtered_entities, summary_stats = entity_filter.filter_entities(metadata_list)
```

### Friendly Name Flow - WORKING STATUS ✅
**Investigation Results**: Complete flow works perfectly:
1. ✅ Database query extracts: `'Wärmepumpe - Tagesverbrauch'`
2. ✅ Entity filtering preserves friendly names  
3. ✅ InfluxDB point creation stores as fields correctly

**Data Schema Confirmed**:
- **Tags**: `entity_id`, `domain`, `category`, `unit`, `source`
- **Fields**: `value` (consistent with HA native integration)

**Issue**: Existing InfluxDB data exported with old/broken code - need fresh export

## Performance Optimization (2025-01-23)

### Metadata Query Optimization ⚡ IMPLEMENTED
**Problem**: Cartesian Product caused 2.8M rows instead of 433 entities
- JOIN to `states` table without filtering fetched ALL historical state changes (avg 6,540 per entity)
- OFFSET pagination over 2.8M rows was extremely inefficient
- ~60 second startup delay before actual export

**Solution**: Subquery for latest attributes only
```sql
LEFT JOIN (
    SELECT s.metadata_id, s.attributes_id
    FROM states s
    INNER JOIN (
        SELECT metadata_id, MAX(last_updated_ts) as max_ts
        FROM states WHERE attributes_id IS NOT NULL
        GROUP BY metadata_id
    ) latest_ts ON s.metadata_id = latest_ts.metadata_id
                AND s.last_updated_ts = latest_ts.max_ts
    WHERE s.attributes_id IS NOT NULL
    GROUP BY s.metadata_id
) latest ON stm.metadata_id = latest.metadata_id
```

**Results**:
- ✅ **13x speedup**: 60s → 4.75s for metadata loading
- ✅ 2.8M rows → 433 entities (99.98% reduction)
- ✅ No duplicates (GROUP BY ensures uniqueness)
- ✅ 100% friendly_name coverage

**Changed Functions** (src/database.py):
- `iter_statistics_metadata()` - Lines 284-367
- `get_statistics_metadata()` - Lines 97-166
- `get_statistics_metadata_count()` - Lines 262-274

### Surprising Discovery: OFFSET is Fast with Covering Indexes

**Initial Assumption**: OFFSET pagination over 1.4M statistics records would be slow
**Reality**: OFFSET is extremely efficient with proper indexes!

**Benchmark** (689k records, all entities):
- OFFSET: 7.66s (90k records/sec)
- Keyset: 11.95s (58k records/sec)
- **OFFSET is 1.56x FASTER!**

**Why?**
- `ix_statistics_statistic_id_start_ts (metadata_id, start_ts)` is a **COVERING INDEX**
- Contains all SELECT fields directly in the index
- No disk I/O for row data needed
- SQLite can "jump" in the index efficiently for moderate offsets

**Decision**: Keep OFFSET pagination for statistics, Keyset not needed

**Lesson**: Don't optimize prematurely - measure first! A perfect index can make "slow" operations fast.

### Counter Entity Statistics Requirement

**Discovery**: Counter entities (e.g., `counter.garagenoffnungen`) don't appear in export

**Root Cause**: Home Assistant only creates Long-Term Statistics for entities with `state_class`
- Counter domain has NO `state_class` by default
- No `state_class` = no entry in `statistics_meta`
- Only 433/1172 entities (37%) have statistics

**Solution for Users**: Add to `configuration.yaml`:
```yaml
homeassistant:
  customize:
    counter.garagenoffnungen:
      state_class: total_increasing
      unit_of_measurement: "Öffnungen"
```

**Important**: This only creates statistics going forward, not retroactively!

### Overall Performance Improvement

**Before Optimization**:
- Metadata Loading: ~60s
- Statistics Query: ~8s
- Processing: ~25s
- InfluxDB Write: ~40s
- **Total: ~133s**

**After Optimization**:
- Metadata Loading: ~5s (13x faster)
- Statistics Query: ~8s (unchanged - already optimal)
- Processing: ~25s
- InfluxDB Write: ~40s
- **Total: ~78s**

**Speedup: 1.7x (saved ~55 seconds per export)**

## Commands for User Reference

### Analysis
```bash
python scripts/export.py analyze-entities --verbose
```

### Setup & Migration
```bash
python scripts/cleanup_exported_data.py --migrated  # Remove old migrated data
python scripts/export.py export                     # Fresh export with friendly names
```

### Bucket Analysis
```bash
python scripts/estimate_bucket_size.py
```

## Success Metrics
- Domain filtering: Predictable entity inclusion
- Unit compatibility: Seamless HA integration  
- Data richness: Enhanced metadata for better queries
- Future-proof: Handles new sensor types automatically
- Use venv
- Do not delete *.db or *.env files that the user might want to use later still