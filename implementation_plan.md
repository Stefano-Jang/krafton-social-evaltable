# 구현 계획

## 개요
Databricks notebook에서 실행할 Python 코드를 작성하여 두 개의 Delta 테이블을 생성하고 각각 500개의 샘플 데이터를 삽입합니다.

## 테이블 정보
- **Catalog**: `main_dev`
- **Schema**: `databricks_support`
- **테이블**:
  1. `main_dev.databricks_support.current_model_eval`
  2. `main_dev.databricks_support.challenge_model_eval`

---

## 구현 단계

### 1. 프로젝트 구조 설정
```
krafton-social-evaltable/
├── prd.md
├── implementation_qna.md
├── implementation_plan.md (현재 파일)
└── databricks_notebook.py (생성할 파일)
```

### 2. 공통 설정 및 유틸리티 함수 구현

#### 2.1 필요한 라이브러리 임포트
- `random`: 랜덤 데이터 생성
- `datetime`, `timedelta`: 타임스탬프 처리
- `json`: eval_json 생성
- Databricks/PySpark 관련 라이브러리

#### 2.2 상수 정의
- 모델 이름 리스트 (22개)
- eval_model 고정 값: `"['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro']"`
- type 리스트: `['EXTRACT', 'MATCH', 'COMPARE', 'SUMMARY']`
- 시작 타임스탬프: `2025-12-15 00:00:00 UTC`

#### 2.3 유틸리티 함수
- `generate_realistic_stats()`: avg, min, max, std 생성
  - min < avg < max 보장
  - std는 적절한 범위 내에서 생성
  - 모든 값 < 1.0
- `select_win_model()`: challenge_model_eval의 win_model 선택 로직
  - a. avg가 가장 높은 모델
  - b. avg 동일 시 std가 낮은 모델
  - c. avg, std 동일 시 task_duration_sec가 낮은 모델

### 3. current_model_eval 테이블 구현

#### 3.1 테이블 스키마
```sql
CREATE TABLE IF NOT EXISTS main_dev.databricks_support.current_model_eval (
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
```

#### 3.2 데이터 생성 로직 (500개 row = 125개 시간대 × 4개 type)
각 시간대마다 4개의 type (EXTRACT, MATCH, COMPARE, SUMMARY)을 각각 하나씩 생성:
- `id`: auto increment (테이블 생성 시 자동)
- `type`: 동일한 test_set_from에 대해 ['EXTRACT', 'MATCH', 'COMPARE', 'SUMMARY'] 각각 하나씩
  - 예: test_set_from `2025-12-15 00:00:00`일 때 4개 row 생성 (각 type별 1개)
  - 예: test_set_from `2025-12-15 01:00:00`일 때 4개 row 생성 (각 type별 1개)
  - ... 125개 시간대 반복
- `eval_model`: `"['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro']"` 고정
- `test_set_from`: 
  - 시작: `2025-12-15 00:00:00`
  - 125개 시간대: 0시, 1시, 2시, ... 124시 (1시간씩 증가, 약 5일간)
  - 각 시간대마다 4개 row (type별 1개)
- `test_set_to`: `test_set_from + 1시간`
- `test_set_size`: 5000 ~ 30000 사이 랜덤 정수
- `eval_json`:
  ```json
  {
    "win_model": "<22개 모델 중 랜덤 선택>",
    "eval": [
      {
        "model_name": "<win_model과 동일>",
        "avg": <0 ~ 1 사이 랜덤>,
        "min": <avg보다 작은 값>,
        "max": <avg보다 큰 값, 1 미만>,
        "std": <적절한 표준편차>,
        "task_duration_sec": <10 ~ 300 사이 랜덤 정수>
      }
    ]
  }
  ```
- `created_at`: `test_set_to + 30분`

### 4. challenge_model_eval 테이블 구현

#### 4.1 테이블 스키마
```sql
CREATE TABLE IF NOT EXISTS main_dev.databricks_support.challenge_model_eval (
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
```

#### 4.2 데이터 생성 로직 (500개 row = 125개 시간대 × 4개 type)
각 시간대마다 4개의 type (EXTRACT, MATCH, COMPARE, SUMMARY)을 각각 하나씩 생성:
- `id`: auto increment
- `type`: 동일한 test_set_from에 대해 ['EXTRACT', 'MATCH', 'COMPARE', 'SUMMARY'] 각각 하나씩
  - 예: test_set_from `2025-12-15 00:00:00`일 때 4개 row 생성 (각 type별 1개)
  - 예: test_set_from `2025-12-15 06:00:00`일 때 4개 row 생성 (각 type별 1개)
  - ... 125개 시간대 반복
- `eval_model`: `"['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro']"` 고정
- `test_set_from`: 
  - 시작: `2025-12-15 00:00:00`
  - 125개 시간대: 0시, 6시, 12시, 18시, 24시(다음날 0시), 6시, ... (6시간씩 증가, 약 31일간)
  - 각 시간대마다 4개 row (type별 1개)
- `test_set_to`: `test_set_from + 6시간`
- `test_set_size`: 5000 ~ 30000 사이 랜덤 정수
- `eval_json`:
  ```json
  {
    "win_model": "<select_win_model() 함수로 결정>",
    "eval": [
      {
        "model_name": "<22개 중 랜덤 선택된 5개 모델>",
        "avg": <0 ~ 1 사이 랜덤>,
        "min": <avg보다 작은 값>,
        "max": <avg보다 큰 값, 1 미만>,
        "std": <적절한 표준편차>,
        "task_duration_sec": <10 ~ 300 사이 랜덤 정수>
      },
      // ... 총 5개의 모델 평가 결과
    ]
  }
  ```
  - **중요**: win_model은 eval 배열의 5개 모델 중에서 규칙에 따라 선택
- `created_at`: `test_set_to + 30분`

### 5. 데이터 삽입 전략

#### 5.1 접근 방법
1. **DataFrame 생성**: PySpark DataFrame을 사용하여 데이터 생성
2. **배치 삽입**: 100개의 row를 리스트로 생성 후 한 번에 DataFrame으로 변환
3. **Delta 테이블 저장**: `saveAsTable()` 메서드 사용

#### 5.2 코드 구조
```python
# 1. 테이블 생성 (존재하지 않으면)
spark.sql("CREATE TABLE IF NOT EXISTS ...")

# 2. 데이터 생성 (500개 row = 125개 시간대 × 4개 type)
data = []
types = ['EXTRACT', 'MATCH', 'COMPARE', 'SUMMARY']

# current_model_eval의 경우
for hour_offset in range(125):  # 125개 시간대
    for type_value in types:  # 각 시간대마다 4개 type
        row = generate_row_for_current_model_eval(hour_offset, type_value)
        data.append(row)

# challenge_model_eval의 경우
for time_offset in range(125):  # 125개 시간대
    for type_value in types:  # 각 시간대마다 4개 type
        row = generate_row_for_challenge_model_eval(time_offset, type_value)
        data.append(row)

# 3. DataFrame 생성 및 저장
df = spark.createDataFrame(data, schema)
df.write.mode("append").saveAsTable("main_dev.databricks_support.current_model_eval")
```

### 6. 검증 및 테스트

#### 6.1 데이터 검증
- 각 테이블에 정확히 500개 row가 삽입되었는지 확인
- **type 분포 검증**:
  - 각 테이블에 EXTRACT, MATCH, COMPARE, SUMMARY가 각각 125개씩 존재하는지 확인
  - 동일한 test_set_from에 4개의 type이 모두 존재하는지 확인
- `eval_json` 내부 데이터 일관성 검증:
  - current_model_eval: win_model == eval[0].model_name
  - challenge_model_eval: win_model이 eval 배열의 5개 모델 중 하나이며 규칙에 맞게 선택되었는지
- 시간 필드 검증:
  - test_set_from < test_set_to
  - test_set_to < created_at
  - current_model_eval: 125개의 고유한 test_set_from 값 (1시간씩 증가)
  - challenge_model_eval: 125개의 고유한 test_set_from 값 (6시간씩 증가)
  - 각 test_set_from마다 정확히 4개의 row (type별 1개)
- 통계 값 검증:
  - min ≤ avg ≤ max
  - 모든 값 < 1.0

#### 6.2 쿼리 예시
```sql
-- Row 수 확인
SELECT COUNT(*) FROM main_dev.databricks_support.current_model_eval;
SELECT COUNT(*) FROM main_dev.databricks_support.challenge_model_eval;

-- type별 분포 확인 (각각 25개씩 있어야 함)
SELECT type, COUNT(*) as cnt 
FROM main_dev.databricks_support.current_model_eval 
GROUP BY type 
ORDER BY type;

SELECT type, COUNT(*) as cnt 
FROM main_dev.databricks_support.challenge_model_eval 
GROUP BY type 
ORDER BY type;

-- test_set_from별 row 수 확인 (각각 4개씩 있어야 함)
SELECT test_set_from, COUNT(*) as cnt
FROM main_dev.databricks_support.current_model_eval
GROUP BY test_set_from
ORDER BY test_set_from;

SELECT test_set_from, COUNT(*) as cnt
FROM main_dev.databricks_support.challenge_model_eval
GROUP BY test_set_from
ORDER BY test_set_from;

-- test_set_from과 type 조합 확인
SELECT test_set_from, type
FROM main_dev.databricks_support.current_model_eval
ORDER BY test_set_from, type
LIMIT 20;

-- 샘플 데이터 확인
SELECT * FROM main_dev.databricks_support.current_model_eval LIMIT 5;
SELECT * FROM main_dev.databricks_support.challenge_model_eval LIMIT 5;

-- 시간 필드 검증
SELECT 
  test_set_from, 
  test_set_to, 
  created_at,
  TIMESTAMPDIFF(HOUR, test_set_from, test_set_to) as hour_diff
FROM main_dev.databricks_support.current_model_eval
LIMIT 10;
```

---

## 구현 파일 구조

### databricks_notebook.py
```
1. 임포트 및 상수 정의
   - 라이브러리 임포트
   - 모델 리스트, type 리스트, eval_model 고정값 등

2. 유틸리티 함수
   - generate_realistic_stats()
   - select_win_model()
   - generate_current_model_eval_row()
   - generate_challenge_model_eval_row()

3. 테이블 생성 함수
   - create_tables()

4. 데이터 생성 및 삽입 함수
   - insert_current_model_eval_data()
   - insert_challenge_model_eval_data()

5. 메인 실행 로직
   - create_tables()
   - insert_current_model_eval_data()
   - insert_challenge_model_eval_data()
   - 검증 쿼리 실행

6. 결과 출력
   - 각 테이블의 row 수 출력
   - 샘플 데이터 출력
```

---

## 주의사항

1. **Liquid Clustering**: 테이블을 `CLUSTER BY (type)`으로 생성하여 Liquid Clustering을 활성화합니다. 이는 `type` 컬럼을 기준으로 데이터를 자동으로 클러스터링하여 쿼리 성능을 향상시킵니다. 특히 type별로 데이터를 조회할 때 큰 성능 이점을 얻을 수 있습니다.

2. **type별 데이터 분포**: 각 test_set_from 시간대마다 4개의 type (EXTRACT, MATCH, COMPARE, SUMMARY)이 각각 하나씩 생성됩니다. 따라서:
   - current_model_eval: 125개 시간대 × 4개 type = 500개 row
   - challenge_model_eval: 125개 시간대 × 4개 type = 500개 row
   - 각 type은 정확히 125개씩 존재해야 합니다.

3. **VARIANT 타입**: Databricks의 VARIANT 타입은 JSON 데이터를 저장하는 semi-structured 데이터 타입입니다. Python dict를 JSON 문자열로 변환하여 저장해야 할 수 있습니다.

4. **AUTO INCREMENT**: Databricks에서는 `GENERATED ALWAYS AS IDENTITY`를 사용하여 auto increment를 구현합니다. 데이터 삽입 시 id 컬럼은 제외해야 합니다.

5. **타임스탬프**: UTC 기준으로 생성하며, Databricks의 `TIMESTAMP` 타입은 timezone-aware입니다.

6. **랜덤 시드**: 재현 가능한 결과를 원한다면 `random.seed()` 설정을 고려할 수 있습니다.

7. **eval_model 컬럼**: 문자열로 저장되는 리스트 형태이므로 `"['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro']"` 형태의 문자열로 저장합니다.

---

## 다음 단계

이 구현 계획을 바탕으로 `databricks_notebook.py` 파일을 생성하여 실제 코드를 작성하겠습니다.

