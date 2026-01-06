1. test_set_to 시간 계산
current_model_eval: "test_set_from 부터 +1 시간"이라고 되어 있는데, 예시는 2025-12-15 01:00:00로 되어 있습니다. 이것은 test_set_from이 2025-12-15 00:00:00일 때 맞나요?
응답: 맞습니다

challenge_model_eval: "test_set_from 부터 +6 시간"이라고 되어 있는데, 예시도 2025-12-15 01:00:00로 되어 있습니다. 이건 2025-12-15 06:00:00이 되어야 하는 것 아닌가요?
응답: 네 맞습니다. 오타 수정했습니다.

2. eval_model 컬럼 값
eval_model: ['databricks-gpt-5-2', 'databricks-claude-sonnet-4-5', 'databricks-gemini-3-pro'] 고정이라고 되어 있는데:
각 row마다 이 3개 중 하나를 선택하는 건가요?
아니면 이 3개를 모두 포함하는 array/list 형태로 저장하는 건가요?
응답: 네 저 list형태로 된 STRING으로 고정해 달라는 것입니다.

3. challenge_model_eval의 eval_json.eval[] 배열
"eval.json.eval[]의 배열 크기는 항상 5 (model_name 다섯개 포함)"라고 되어 있는데:
모델 이름 후보가 22개인데, 어떤 기준으로 5개를 선택하나요?
랜덤으로 5개를 선택하는 건가요?
아니면 특정 조합이나 규칙이 있나요?
응답: 랜덤으로 다섯개를 선택하면 됩니다.

4. Databricks 환경 설정
어떤 catalog와 schema를 사용해야 하나요?
테이블의 전체 경로는 어떻게 되나요? (예: catalog.schema.table_name)
응답: catalog는 main_dev, schema는 databricks_support를 사용하면 됩니다. 테이블 전체 경로는 각각 main_dev.databricks_support.current_model_eval, main_dev.databricks_support.challenge_model_eval 입니다.

5. 데이터 삽입 수량 확인
"해당 테이블에 규칙에 따른 example data를 각각 100개씩 insert"라는 것은:
current_model_eval 테이블에 100개 row
challenge_model_eval 테이블에 100개 row
응답: 네 맞습니다.
