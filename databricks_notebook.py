# Databricks notebook source
# MAGIC %md
# MAGIC # LLM Model Evaluation Tables Generation
# MAGIC 
# MAGIC This notebook creates two Delta tables for LLM model evaluation results:
# MAGIC 1. `main_dev.databricks_support.current_model_eval` - Current model evaluation results
# MAGIC 2. `main_dev.databricks_support.challenge_model_eval` - Challenge model evaluation results (multiple models)
# MAGIC 
# MAGIC Each table will contain 500 rows (125 time periods × 4 types)

# COMMAND ----------

import random
import json
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Constants and Configuration

# COMMAND ----------

# 모델 이름 리스트 (22개)
MODEL_NAMES = [
    'databricks-gpt-5-2',
    'databricks-gemini-3-flash',
    'databricks-gpt-5-1',
    'databricks-gemini-3-pro',
    'databricks-gpt-5',
    'databricks-gemini-2-5-flash',
    'databricks-claude-sonnet-4-5',
    'databricks-claude-haiku-4-5',
    'databricks-gpt-oss-120b',
    'databricks-gpt-5-mini',
    'databricks-gpt-5-nano',
    'databricks-gemini-2-5-pro',
    'databricks-gpt-oss-20b',
    'databricks-qwen3-next-80b-a3b-instruct',
    'databricks-llama-4-maverick',
    'databricks-gemma-3-12b',
    'databricks-meta-llama-3-1-8b-instruct',
    'databricks-meta-llama-3-3-70b-instruct',
    'databricks-claude-opus-4-5',
    'databricks-claude-opus-4-1',
    'databricks-claude-sonnet-4',
    'databricks-claude-3-7-sonnet'
]

# Type 리스트
TYPES = ['EXTRACT', 'MATCH', 'COMPARE', 'SUMMARY']

# eval_model 고정 값 (문자열로 저장되는 리스트)
EVAL_MODEL_FIXED = "['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro']"

# 시작 시간
START_TIME = datetime(2025, 12, 15, 0, 0, 0)

# 테이블 경로
CATALOG = "main_dev"
SCHEMA = "databricks_support"
CURRENT_TABLE = f"{CATALOG}.{SCHEMA}.current_model_eval"
CHALLENGE_TABLE = f"{CATALOG}.{SCHEMA}.challenge_model_eval"

# 데이터 생성 설정
NUM_TIME_PERIODS = 125  # 125개 시간대
NUM_TYPES = 4  # 4개 type
TOTAL_ROWS = NUM_TIME_PERIODS * NUM_TYPES  # 500개 row

print(f"Total rows per table: {TOTAL_ROWS}")
print(f"Time periods: {NUM_TIME_PERIODS}")
print(f"Types per period: {NUM_TYPES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utility Functions

# COMMAND ----------

def generate_realistic_stats():
    """
    통계 값 생성: min < avg < max, 모두 < 1.0
    그럴듯한 통계 값을 생성합니다.
    """
    # avg를 먼저 생성 (0.7 ~ 0.95 사이)
    avg = random.uniform(0.7, 0.95)
    
    # min은 avg보다 작게 (avg - 0.05 ~ avg - 0.15)
    min_diff = random.uniform(0.05, 0.15)
    min_val = max(0.0, avg - min_diff)
    
    # max는 avg보다 크게 (avg + 0.02 ~ avg + 0.08), 단 1.0 미만
    max_diff = random.uniform(0.02, 0.08)
    max_val = min(0.999, avg + max_diff)
    
    # std는 적절한 범위 (0.02 ~ 0.10)
    std = random.uniform(0.02, 0.10)
    
    # 소수점 2자리로 반올림
    return {
        'avg': round(avg, 2),
        'min': round(min_val, 2),
        'max': round(max_val, 2),
        'std': round(std, 2)
    }

def select_win_model(eval_list):
    """
    challenge_model_eval의 win_model 선택 로직
    a. avg가 가장 높은 모델
    b. avg가 같으면 std가 낮은 모델
    c. avg와 std가 모두 같으면 task_duration_sec가 낮은 모델
    """
    # 정렬: avg 내림차순 → std 오름차순 → task_duration_sec 오름차순
    sorted_eval = sorted(
        eval_list,
        key=lambda x: (-x['avg'], x['std'], x['task_duration_sec'])
    )
    return sorted_eval[0]['model_name']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Generation Functions

# COMMAND ----------

def generate_current_model_eval_row(time_offset, type_value):
    """
    current_model_eval 테이블의 한 row 생성
    
    Args:
        time_offset: 시간 오프셋 (0 ~ 124)
        type_value: type 값 (EXTRACT, MATCH, COMPARE, SUMMARY)
    """
    # test_set_from: 1시간씩 증가
    test_set_from = START_TIME + timedelta(hours=time_offset)
    
    # test_set_to: test_set_from + 1시간
    test_set_to = test_set_from + timedelta(hours=1)
    
    # created_at: test_set_to + 30분
    created_at = test_set_to + timedelta(minutes=30)
    
    # test_set_size: 5000 ~ 30000 사이 랜덤
    test_set_size = random.randint(5000, 30000)
    
    # win_model: 22개 모델 중 랜덤 선택
    win_model = random.choice(MODEL_NAMES)
    
    # 통계 값 생성
    stats = generate_realistic_stats()
    
    # task_duration_sec: 10 ~ 300 사이 랜덤
    task_duration_sec = random.randint(10, 300)
    
    # eval_json 생성
    eval_json = {
        "win_model": win_model,
        "eval": [
            {
                "model_name": win_model,  # win_model과 동일
                "avg": stats['avg'],
                "min": stats['min'],
                "max": stats['max'],
                "std": stats['std'],
                "task_duration_sec": task_duration_sec
            }
        ]
    }
    
    return {
        "type": type_value,
        "eval_model": EVAL_MODEL_FIXED,
        "test_set_from": test_set_from,
        "test_set_to": test_set_to,
        "test_set_size": test_set_size,
        "eval_json": json.dumps(eval_json),
        "created_at": created_at
    }

def generate_challenge_model_eval_row(time_offset, type_value):
    """
    challenge_model_eval 테이블의 한 row 생성
    
    Args:
        time_offset: 시간 오프셋 (0 ~ 124)
        type_value: type 값 (EXTRACT, MATCH, COMPARE, SUMMARY)
    """
    # test_set_from: 6시간씩 증가
    test_set_from = START_TIME + timedelta(hours=time_offset * 6)
    
    # test_set_to: test_set_from + 6시간
    test_set_to = test_set_from + timedelta(hours=6)
    
    # created_at: test_set_to + 30분
    created_at = test_set_to + timedelta(minutes=30)
    
    # test_set_size: 5000 ~ 30000 사이 랜덤
    test_set_size = random.randint(5000, 30000)
    
    # 22개 모델 중 랜덤으로 5개 선택
    selected_models = random.sample(MODEL_NAMES, 5)
    
    # 각 모델에 대한 평가 결과 생성
    eval_list = []
    for model_name in selected_models:
        stats = generate_realistic_stats()
        task_duration_sec = random.randint(10, 300)
        
        eval_list.append({
            "model_name": model_name,
            "avg": stats['avg'],
            "min": stats['min'],
            "max": stats['max'],
            "std": stats['std'],
            "task_duration_sec": task_duration_sec
        })
    
    # win_model 선택
    win_model = select_win_model(eval_list)
    
    # eval_json 생성
    eval_json = {
        "win_model": win_model,
        "eval": eval_list
    }
    
    return {
        "type": type_value,
        "eval_model": EVAL_MODEL_FIXED,
        "test_set_from": test_set_from,
        "test_set_to": test_set_to,
        "test_set_size": test_set_size,
        "eval_json": json.dumps(eval_json),
        "created_at": created_at
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Table Creation

# COMMAND ----------

def create_tables():
    """
    두 개의 테이블을 생성합니다.
    Liquid Clustering을 사용하여 type 컬럼으로 클러스터링합니다.
    """
    
    # current_model_eval 테이블 생성
    create_current_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {CURRENT_TABLE} (
      id BIGINT GENERATED ALWAYS AS IDENTITY,
      type STRING,
      eval_model STRING,
      test_set_from TIMESTAMP,
      test_set_to TIMESTAMP,
      test_set_size INT,
      eval_json VARIANT,
      created_at TIMESTAMP
    )
    CLUSTER BY (type)
    """
    
    print(f"Creating table: {CURRENT_TABLE}")
    spark.sql(create_current_table_sql)
    print(f"✓ Table created: {CURRENT_TABLE}")
    
    # challenge_model_eval 테이블 생성
    create_challenge_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {CHALLENGE_TABLE} (
      id BIGINT GENERATED ALWAYS AS IDENTITY,
      type STRING,
      eval_model STRING,
      test_set_from TIMESTAMP,
      test_set_to TIMESTAMP,
      test_set_size INT,
      eval_json VARIANT,
      created_at TIMESTAMP
    )
    CLUSTER BY (type)
    """
    
    print(f"Creating table: {CHALLENGE_TABLE}")
    spark.sql(create_challenge_table_sql)
    print(f"✓ Table created: {CHALLENGE_TABLE}")

# 테이블 생성 실행
create_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Generation and Insertion

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Generate current_model_eval data

# COMMAND ----------

def insert_current_model_eval_data():
    """
    current_model_eval 테이블에 500개 row 삽입
    125개 시간대 × 4개 type
    """
    print(f"Generating {TOTAL_ROWS} rows for {CURRENT_TABLE}...")
    
    data = []
    
    # 125개 시간대
    for time_offset in range(NUM_TIME_PERIODS):
        # 각 시간대마다 4개 type
        for type_value in TYPES:
            row = generate_current_model_eval_row(time_offset, type_value)
            data.append(row)
    
    print(f"Generated {len(data)} rows")
    
    # 스키마 정의
    schema = StructType([
        StructField("type", StringType(), False),
        StructField("eval_model", StringType(), False),
        StructField("test_set_from", TimestampType(), False),
        StructField("test_set_to", TimestampType(), False),
        StructField("test_set_size", IntegerType(), False),
        StructField("eval_json", StringType(), False),
        StructField("created_at", TimestampType(), False)
    ])
    
    # DataFrame 생성 (스키마 명시)
    df = spark.createDataFrame(data, schema)
    
    # eval_json을 VARIANT 타입으로 변환
    df = df.selectExpr(
        "type",
        "eval_model",
        "test_set_from",
        "test_set_to",
        "test_set_size",
        "parse_json(eval_json) as eval_json",
        "created_at"
    )
    
    # 테이블에 삽입
    print(f"Inserting data into {CURRENT_TABLE}...")
    df.write.mode("append").saveAsTable(CURRENT_TABLE)
    
    print(f"✓ Successfully inserted {len(data)} rows into {CURRENT_TABLE}")
    
    return len(data)

# current_model_eval 데이터 삽입
current_rows = insert_current_model_eval_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Generate challenge_model_eval data

# COMMAND ----------

def insert_challenge_model_eval_data():
    """
    challenge_model_eval 테이블에 500개 row 삽입
    125개 시간대 × 4개 type
    """
    print(f"Generating {TOTAL_ROWS} rows for {CHALLENGE_TABLE}...")
    
    data = []
    
    # 125개 시간대
    for time_offset in range(NUM_TIME_PERIODS):
        # 각 시간대마다 4개 type
        for type_value in TYPES:
            row = generate_challenge_model_eval_row(time_offset, type_value)
            data.append(row)
    
    print(f"Generated {len(data)} rows")
    
    # 스키마 정의
    schema = StructType([
        StructField("type", StringType(), False),
        StructField("eval_model", StringType(), False),
        StructField("test_set_from", TimestampType(), False),
        StructField("test_set_to", TimestampType(), False),
        StructField("test_set_size", IntegerType(), False),
        StructField("eval_json", StringType(), False),
        StructField("created_at", TimestampType(), False)
    ])
    
    # DataFrame 생성 (스키마 명시)
    df = spark.createDataFrame(data, schema)
    
    # eval_json을 VARIANT 타입으로 변환
    df = df.selectExpr(
        "type",
        "eval_model",
        "test_set_from",
        "test_set_to",
        "test_set_size",
        "parse_json(eval_json) as eval_json",
        "created_at"
    )
    
    # 테이블에 삽입
    print(f"Inserting data into {CHALLENGE_TABLE}...")
    df.write.mode("append").saveAsTable(CHALLENGE_TABLE)
    
    print(f"✓ Successfully inserted {len(data)} rows into {CHALLENGE_TABLE}")
    
    return len(data)

# challenge_model_eval 데이터 삽입
challenge_rows = insert_challenge_model_eval_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Validation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Row Count Validation

# COMMAND ----------

print("=" * 80)
print("ROW COUNT VALIDATION")
print("=" * 80)

# current_model_eval row 수 확인
current_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CURRENT_TABLE}").collect()[0]['cnt']
print(f"\n{CURRENT_TABLE}: {current_count} rows (expected: {TOTAL_ROWS})")
if current_count == TOTAL_ROWS:
    print("✓ Row count is correct!")
else:
    print(f"✗ Row count mismatch! Expected {TOTAL_ROWS}, got {current_count}")

# challenge_model_eval row 수 확인
challenge_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CHALLENGE_TABLE}").collect()[0]['cnt']
print(f"\n{CHALLENGE_TABLE}: {challenge_count} rows (expected: {TOTAL_ROWS})")
if challenge_count == TOTAL_ROWS:
    print("✓ Row count is correct!")
else:
    print(f"✗ Row count mismatch! Expected {TOTAL_ROWS}, got {challenge_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Type Distribution Validation

# COMMAND ----------

print("=" * 80)
print("TYPE DISTRIBUTION VALIDATION")
print("=" * 80)

# current_model_eval type 분포
print(f"\n{CURRENT_TABLE} - Type distribution:")
current_type_dist = spark.sql(f"""
    SELECT type, COUNT(*) as cnt 
    FROM {CURRENT_TABLE} 
    GROUP BY type 
    ORDER BY type
""")
current_type_dist.show()

expected_per_type = NUM_TIME_PERIODS
print(f"Expected per type: {expected_per_type}")

# challenge_model_eval type 분포
print(f"\n{CHALLENGE_TABLE} - Type distribution:")
challenge_type_dist = spark.sql(f"""
    SELECT type, COUNT(*) as cnt 
    FROM {CHALLENGE_TABLE} 
    GROUP BY type 
    ORDER BY type
""")
challenge_type_dist.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Test Set Time Period Validation

# COMMAND ----------

print("=" * 80)
print("TEST SET TIME PERIOD VALIDATION")
print("=" * 80)

# current_model_eval - test_set_from별 row 수 (각 4개씩 있어야 함)
print(f"\n{CURRENT_TABLE} - Rows per test_set_from (first 10):")
spark.sql(f"""
    SELECT test_set_from, COUNT(*) as cnt
    FROM {CURRENT_TABLE}
    GROUP BY test_set_from
    ORDER BY test_set_from
    LIMIT 10
""").show()

# current_model_eval - 고유한 test_set_from 개수
current_unique_times = spark.sql(f"""
    SELECT COUNT(DISTINCT test_set_from) as unique_times
    FROM {CURRENT_TABLE}
""").collect()[0]['unique_times']
print(f"Unique test_set_from values: {current_unique_times} (expected: {NUM_TIME_PERIODS})")

# challenge_model_eval - test_set_from별 row 수 (각 4개씩 있어야 함)
print(f"\n{CHALLENGE_TABLE} - Rows per test_set_from (first 10):")
spark.sql(f"""
    SELECT test_set_from, COUNT(*) as cnt
    FROM {CHALLENGE_TABLE}
    GROUP BY test_set_from
    ORDER BY test_set_from
    LIMIT 10
""").show()

# challenge_model_eval - 고유한 test_set_from 개수
challenge_unique_times = spark.sql(f"""
    SELECT COUNT(DISTINCT test_set_from) as unique_times
    FROM {CHALLENGE_TABLE}
""").collect()[0]['unique_times']
print(f"Unique test_set_from values: {challenge_unique_times} (expected: {NUM_TIME_PERIODS})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Test Set Time and Type Combination

# COMMAND ----------

print("=" * 80)
print("TEST SET TIME AND TYPE COMBINATION")
print("=" * 80)

print(f"\n{CURRENT_TABLE} - First 20 rows (test_set_from, type):")
spark.sql(f"""
    SELECT test_set_from, type
    FROM {CURRENT_TABLE}
    ORDER BY test_set_from, type
    LIMIT 20
""").show()

print(f"\n{CHALLENGE_TABLE} - First 20 rows (test_set_from, type):")
spark.sql(f"""
    SELECT test_set_from, type
    FROM {CHALLENGE_TABLE}
    ORDER BY test_set_from, type
    LIMIT 20
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.5 Sample Data Preview

# COMMAND ----------

print("=" * 80)
print("SAMPLE DATA PREVIEW")
print("=" * 80)

print(f"\n{CURRENT_TABLE} - Sample rows:")
spark.sql(f"SELECT * FROM {CURRENT_TABLE} LIMIT 3").show(truncate=False, vertical=True)

print(f"\n{CHALLENGE_TABLE} - Sample rows:")
spark.sql(f"SELECT * FROM {CHALLENGE_TABLE} LIMIT 3").show(truncate=False, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.6 Time Field Validation

# COMMAND ----------

print("=" * 80)
print("TIME FIELD VALIDATION")
print("=" * 80)

# current_model_eval - 시간 필드 검증
print(f"\n{CURRENT_TABLE} - Time field validation (first 10):")
spark.sql(f"""
    SELECT 
        test_set_from, 
        test_set_to, 
        created_at,
        (unix_timestamp(test_set_to) - unix_timestamp(test_set_from)) / 3600 as hours_diff_set,
        (unix_timestamp(created_at) - unix_timestamp(test_set_to)) / 60 as minutes_diff_created
    FROM {CURRENT_TABLE}
    ORDER BY test_set_from
    LIMIT 10
""").show()

# challenge_model_eval - 시간 필드 검증
print(f"\n{CHALLENGE_TABLE} - Time field validation (first 10):")
spark.sql(f"""
    SELECT 
        test_set_from, 
        test_set_to, 
        created_at,
        (unix_timestamp(test_set_to) - unix_timestamp(test_set_from)) / 3600 as hours_diff_set,
        (unix_timestamp(created_at) - unix_timestamp(test_set_to)) / 60 as minutes_diff_created
    FROM {CHALLENGE_TABLE}
    ORDER BY test_set_from
    LIMIT 10
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Summary

# COMMAND ----------

print("=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"""
✓ Successfully created and populated two tables:
  - {CURRENT_TABLE}: {current_count} rows
  - {CHALLENGE_TABLE}: {challenge_count} rows

✓ Data structure:
  - {NUM_TIME_PERIODS} time periods
  - {NUM_TYPES} types per period (EXTRACT, MATCH, COMPARE, SUMMARY)
  - Total: {TOTAL_ROWS} rows per table

✓ current_model_eval characteristics:
  - Time interval: 1 hour
  - eval_json.eval: 1 model per row
  - Period coverage: ~5 days

✓ challenge_model_eval characteristics:
  - Time interval: 6 hours
  - eval_json.eval: 5 models per row (randomly selected)
  - Period coverage: ~31 days

✓ Both tables use Liquid Clustering on 'type' column for optimized query performance
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Additional Queries (Optional)

# COMMAND ----------

# 평가 결과가 저장된 테이블을 쿼리할 수 있는 예시 쿼리들

# Type별 평균 test_set_size
print("\nAverage test_set_size by type:")
spark.sql(f"""
    SELECT 
        type,
        AVG(test_set_size) as avg_test_set_size,
        MIN(test_set_size) as min_test_set_size,
        MAX(test_set_size) as max_test_set_size
    FROM {CURRENT_TABLE}
    GROUP BY type
    ORDER BY type
""").show()

# COMMAND ----------

# 날짜별 평가 건수
print("\nEvaluation count by date:")
spark.sql(f"""
    SELECT 
        DATE(test_set_from) as eval_date,
        COUNT(*) as eval_count
    FROM {CURRENT_TABLE}
    GROUP BY DATE(test_set_from)
    ORDER BY eval_date
    LIMIT 10
""").show()

# COMMAND ----------

print("\n" + "=" * 80)
print("✓ Notebook execution completed successfully!")
print("=" * 80)

