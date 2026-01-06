- Databricks notebook에서 사용할 python code를 작성해야 함

- 정의된 형태의 delta table을 생성하고 해당 테이블에 규칙에 따른 example data를 각각 100개씩 insert 해야 함

- 추가할 테이블 명은 아래 두가지
1. current_model_eval: 현재 LLM 모델의 성능 평가 결과를 기록하는 용도
2. challenge_model_eval: 현재 LLM 모델 뿐만 아니라 다른 LLM 모델의 결과도 모두 평가한 결과를 기록하는 용도

- 두 테이블의 스키마 정의는 동일하고 아래와 같다.
id: BIGINT, auto increment
type: STRING, 
eval_model: STRING,
test_set_from: TIMESTAMP(UTC),
test_set_to: TIMESTAMP(UTC),
test_set_size: INT,
eval_json: VARIANT,
created_at: TIMESTAMP(UTC)

- 테이블의 각 컬럼에 들어갈 내용과 규칙은 아래와 같다.
1. current_model_eval

id: auto increment하는 BIGINT 숫자 (예: 123)
type: EXTRACT, MATCH, COMPARE, SUMMARY 넷 중의 하나 (예: EXTRACT)
eval_model: ['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro'] 고정
test_set_from: 2025-12-15 00:00:00 (동일한 test_set_from은 한번만, 이후부터는 1시간씩 증가하면서 추가)
test_set_to: 2025-12-15 01:00:00 (test_set_from 부터 +1 시간)
test_set_size: 5000에서 30000 사이의 random 숫자
eval_json: 아래 형태 
{
  "win_model": "databricks-gpt-5.2",
  "eval": [
    {
      "model_name": "databricks-gpt-5.2",
      "avg": 0.90,
      "min": 0.85,
      "max": 1.00,
      "std": 0.05,
      "task_duration_sec": 35,
    }
  ]
}
- eval_json.win_model은 아래 모델 중의 하나 (모델 이름 후보)
databricks-gpt-5-2
databricks-gemini-3-flash
databricks-gpt-5-1
databricks-gemini-3-pro
databricks-gpt-5
databricks-gemini-2-5-flash
databricks-claude-sonnet-4-5
databricks-claude-haiku-4-5
databricks-gpt-oss-120b
databricks-gpt-5-mini
databricks-gpt-5-nano
databricks-gemini-2-5-pro
databricks-gpt-oss-20b
databricks-qwen3-next-80b-a3b-instruct
databricks-llama-4-maverick
databricks-gemma-3-12b
databricks-meta-llama-3-1-8b-instruct
databricks-meta-llama-3-3-70b-instruct
databricks-claude-opus-4-5
databricks-claude-opus-4-1
databricks-claude-sonnet-4
databricks-claude-3-7-sonnet

- eval.json.win_model은 eval_json.eval[0].model_name과 같아야 함
- eval.json.eval[0].task_duration_sec는 10에서 300사이의 랜덤 숫자
- eval.json.eval[]의 배열 크기는 항상 1 (model_name 한개만 포함)
- eval.json.eval[0].avg, min, max, std는 모두 1보다 작아야 함. 그럴듯한 숫자여야 함. (예를 들어 min이 max보다 크지 않아야 함)

created_at: 2025-12-15 01:30:00 (test_set_to에서 30분 더한 값)


2. challenge_model_eval
test_set_from, test_set_to, eval_json 정의만 다르고 다른 컬럼 정의는 current_model_eval과 동일

test_set_from: 2025-12-15 00:00:00 (동일한 test_set_from은 한번만, 이후부터는 6시간씩 증가하면서 추가)
test_set_to: 2025-12-15 06:00:00 (test_set_from 부터 +6 시간)
eval_json: 아래 형태 
{
  "win_model": "databricks-gpt-5.2",
  "eval": [
    {
      "model_name": "databricks-gpt-5.2",
      "avg": 0.90,
      "min": 0.85,
      "max": 1.00,
      "std": 0.05,
      "task_duration_sec": 35,
    },
    {
      "model_name": "databricks-gpt-5.1",
      "avg": 0.90,
      "min": 0.85,
      "max": 1.00,
      "std": 0.04,
      "task_duration_sec": 125,

    },
    {
      "model_name": "databricks-gemini-3-flash",
      "avg": 0.89,
      "min": 0.85,
      "max": 1.00,
      "std": 0.05,
      "task_duration_sec": 300,
    },
  ]
}
- eval_json.win_model은 아래 모델 중의 하나 (모델 이름 후보)
databricks-gpt-5-2
databricks-gemini-3-flash
databricks-gpt-5-1
databricks-gemini-3-pro
databricks-gpt-5
databricks-gemini-2-5-flash
databricks-claude-sonnet-4-5
databricks-claude-haiku-4-5
databricks-gpt-oss-120b
databricks-gpt-5-mini
databricks-gpt-5-nano
databricks-gemini-2-5-pro
databricks-gpt-oss-20b
databricks-qwen3-next-80b-a3b-instruct
databricks-llama-4-maverick
databricks-gemma-3-12b
databricks-meta-llama-3-1-8b-instruct
databricks-meta-llama-3-3-70b-instruct
databricks-claude-opus-4-5
databricks-claude-opus-4-1
databricks-claude-sonnet-4
databricks-claude-3-7-sonnet

- eval.json.win_model은 eval_json.eval list에 있는 element 중 
  a. avg가 가장 높은 모델
  b. avg가 같은 것이 두개 이상이면 그 중 std가 낮은 모델
  c. avg와 std가 모두 같다면 그 중 task_duration_sec가 낮은 모델 
- eval.json.eval[0].task_duration_sec는 10에서 300사이의 랜덤 숫자
- eval.json.eval[]의 배열 크기는 항상 5 (model_name 다섯개 포함)
- avg, min, max, std는 모두 1보다 작아야 함. 그럴듯한 숫자여야 함. (예를 들어 min이 max보다 크지 않아야 함)