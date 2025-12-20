def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
    assert channel_handle == "MRCHEESE"

def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name" 

from airflow.models import DagBag

def test_dags_integrity():
    dag_bag = DagBag()

    # 1. No debe haber errores de importación
    assert dag_bag.import_errors == {}, f"import errors found: {dag_bag.import_errors}"
    print("=========")
    print(dag_bag.dags.keys())

    # 2. Validar que los DAGs esperados estén presentes
    expected_dags_ids = {"produce_json", "update_db", "data_quality"}
    loaded_dags_ids = list(dag_bag.dags.keys())
    print("=========")
    print(dag_bag.dags.keys())

    for dag_id in expected_dags_ids:
        assert dag_id in loaded_dags_ids, f"DAG '{dag_id}' is missing"

    # 3. Validar cantidad de DAGs
    assert dag_bag.size() == 3
    print("=========")
    print(dag_bag.size())

    # 4. Validar cantidad de tasks por DAG
    expected_task_counts = {
        "produce_json": 5, 
        "update_db": 3,
        "data_quality": 2,
    }
    print("=========")
    for dag_id, dag in dag_bag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)
        assert expected_count == actual_count, (
            f"DAG '{dag_id}' has {actual_count} tasks, expected {expected_count}."
        )
        print(dag_id, len(dag.tasks))
