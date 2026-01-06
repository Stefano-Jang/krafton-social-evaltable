# LLM Model Evaluation Tables Generator

Databricks Delta 테이블을 생성하고 LLM 모델 평가 결과 샘플 데이터를 삽입하는 프로젝트입니다.

## 📋 프로젝트 개요

두 개의 Delta 테이블을 생성하고 각각 500개의 샘플 데이터를 삽입합니다:

1. **current_model_eval**: 현재 LLM 모델의 성능 평가 결과
2. **challenge_model_eval**: 여러 LLM 모델을 비교 평가한 결과

## 🗂️ 파일 구조

```
krafton-social-evaltable/
├── prd.md                      # 제품 요구사항 문서
├── implementation_qna.md       # 구현 관련 Q&A
├── implementation_plan.md      # 상세 구현 계획
├── databricks_notebook.py      # Databricks notebook 코드
└── README.md                   # 이 파일
```

## 🚀 사용 방법

### 1. Databricks에서 notebook 생성

1. Databricks workspace에 접속
2. 새 Python notebook 생성
3. `databricks_notebook.py` 파일의 내용을 복사하여 붙여넣기

### 2. Notebook 실행

전체 notebook을 순서대로 실행하거나, 각 셀을 개별적으로 실행할 수 있습니다.

```python
# 모든 셀을 순서대로 실행하면:
# 1. 상수 및 설정 정의
# 2. 유틸리티 함수 정의
# 3. 테이블 생성
# 4. 데이터 생성 및 삽입
# 5. 데이터 검증
```

### 3. 실행 결과 확인

Notebook이 성공적으로 실행되면 다음과 같은 결과를 확인할 수 있습니다:

- ✅ 2개 테이블 생성 완료
- ✅ 각 테이블에 500개 row 삽입 완료
- ✅ 데이터 검증 완료

## 📊 테이블 정보

### 테이블 스키마

두 테이블 모두 동일한 스키마를 사용합니다:

| 컬럼명 | 타입 | 설명 |
|--------|------|------|
| id | BIGINT | Auto increment ID |
| type | STRING | 평가 유형 (EXTRACT, MATCH, COMPARE, SUMMARY) |
| eval_model | STRING | 평가 모델 목록 (고정값) |
| test_set_from | TIMESTAMP | 테스트 세트 시작 시간 |
| test_set_to | TIMESTAMP | 테스트 세트 종료 시간 |
| test_set_size | INT | 테스트 세트 크기 |
| eval_json | VARIANT | 평가 결과 JSON |
| created_at | TIMESTAMP | 생성 시간 |

### 테이블 경로

- **current_model_eval**: `main_dev.databricks_support.current_model_eval`
- **challenge_model_eval**: `main_dev.databricks_support.challenge_model_eval`

### Liquid Clustering

두 테이블 모두 `type` 컬럼으로 Liquid Clustering이 설정되어 있어, type별 쿼리 성능이 최적화되어 있습니다.

## 📈 데이터 특성

### current_model_eval

- **총 row 수**: 500개
- **시간대**: 125개 (1시간 간격, 약 5일)
- **시작 시간**: 2025-12-15 00:00:00
- **Type별 분포**: 각 type당 125개
- **eval_json.eval**: 1개 모델 평가 결과

### challenge_model_eval

- **총 row 수**: 500개
- **시간대**: 125개 (6시간 간격, 약 31일)
- **시작 시간**: 2025-12-15 00:00:00
- **Type별 분포**: 각 type당 125개
- **eval_json.eval**: 5개 모델 평가 결과 (랜덤 선택)

## 🔍 데이터 검증

Notebook 실행 시 자동으로 다음 항목들이 검증됩니다:

1. ✅ Row 수 확인 (각 테이블당 500개)
2. ✅ Type 분포 확인 (각 type당 125개)
3. ✅ test_set_from별 row 수 (각 시간대당 4개)
4. ✅ 시간 필드 일관성 (test_set_from < test_set_to < created_at)
5. ✅ 통계 값 검증 (min ≤ avg ≤ max < 1.0)

## 📝 예시 쿼리

### Type별 row 수 확인

```sql
SELECT type, COUNT(*) as cnt 
FROM main_dev.databricks_support.current_model_eval 
GROUP BY type 
ORDER BY type;
```

### 특정 시간대의 평가 결과 조회

```sql
SELECT test_set_from, type, eval_json
FROM main_dev.databricks_support.current_model_eval
WHERE test_set_from = '2025-12-15 00:00:00'
ORDER BY type;
```

### Type별 평균 test_set_size

```sql
SELECT 
    type,
    AVG(test_set_size) as avg_size,
    MIN(test_set_size) as min_size,
    MAX(test_set_size) as max_size
FROM main_dev.databricks_support.challenge_model_eval
GROUP BY type
ORDER BY type;
```

## 🎯 주요 기능

### 1. Realistic Statistics Generation
- min < avg < max 관계 보장
- 모든 값 < 1.0
- 적절한 표준편차 생성

### 2. Win Model Selection (challenge_model_eval)
다음 우선순위로 승리 모델 선택:
1. avg가 가장 높은 모델
2. avg가 같으면 std가 낮은 모델
3. avg와 std가 모두 같으면 task_duration_sec가 낮은 모델

### 3. Time-based Data Distribution
각 시간대마다 4개 type이 균등하게 분포되어 있어, 시계열 분석에 적합합니다.

## 🔧 커스터마이징

### 데이터 수량 변경

`databricks_notebook.py`의 상수를 수정:

```python
NUM_TIME_PERIODS = 125  # 시간대 수 조정
```

### 시작 시간 변경

```python
START_TIME = datetime(2025, 12, 15, 0, 0, 0)  # 원하는 시작 시간으로 변경
```

### 테이블 경로 변경

```python
CATALOG = "main_dev"         # Catalog 이름
SCHEMA = "databricks_support"  # Schema 이름
```

## 📚 참고 문서

- `prd.md`: 상세한 제품 요구사항
- `implementation_plan.md`: 구현 계획 및 설계 문서
- `implementation_qna.md`: 구현 관련 질의응답

## ⚠️ 주의사항

1. Databricks Runtime이 VARIANT 타입을 지원하는지 확인하세요.
2. Catalog와 Schema에 대한 적절한 권한이 있는지 확인하세요.
3. 테이블이 이미 존재하는 경우 `CREATE TABLE IF NOT EXISTS`로 인해 오류가 발생하지 않지만, 데이터는 append 모드로 추가됩니다.
4. 기존 데이터를 삭제하고 다시 생성하려면 먼저 테이블을 DROP 해야 합니다:

```sql
DROP TABLE IF EXISTS main_dev.databricks_support.current_model_eval;
DROP TABLE IF EXISTS main_dev.databricks_support.challenge_model_eval;
```

## 📞 문의

구현 관련 질문이나 이슈가 있으면 `implementation_qna.md` 파일을 참고하거나 담당자에게 문의하세요.

