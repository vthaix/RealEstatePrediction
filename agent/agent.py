
import requests
AIRFLOW_URL = "http://127.0.0.1:8080"
TOKEN = (
    "eyJhbGciOiJIUzUxMiIsImtpZCI6Im5vdC11c2VkIiwidHlwIjoiSldUIn0."
    "eyJzdWIiOiJBbm9ueW1vdXMiLCJyb2xlIjoiQURNSU4iLCJ0ZWFtcyI6W10sImp0aSI6"
    "IjIwYTJiMzY1YjM0YzQzNDhiOGMxZTE2MWI1Nzg4ZTAzIiwiYXVkIjoiYXBhY2hlLWFpcmZs"
    "b3ciLCJuYmYiOjE3ODg0MzA0NDQsImV4cCI6MTc4ODUxNjg0NCwiaWF0IjoxNzg4NDMwNDQ0"
    "fQ.kD3EEWqilBqcOq0OX8hRE8BRd5PVkVFLQydhmYdFgja7-EwpXN-GFKaq_UyZOK4OmcfUK"
    "n3rTap5jhcZsXNldg"
)

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

DAG_ID = "real_estate_eda_pipeline"


def get_latest_run():
    url = f"{AIRFLOW_URL}/api/v2/dags/{DAG_ID}/dagRuns"

    response = requests.get(
        url,
        headers=HEADERS,
        params={
            "order_by": "-logical_date",
            "limit": 1
        }
    )

    response.raise_for_status()

    data = response.json()

    if not data["dag_runs"]:
        return None

    return data["dag_runs"][0]


def get_failed_tasks(run_id):
    url = (
        f"{AIRFLOW_URL}/api/v2/dags/"
        f"{DAG_ID}/dagRuns/{run_id}/taskInstances"
    )

    response = requests.get(
        url,
        headers=HEADERS
    )

    response.raise_for_status()

    data = response.json()

    return [
        task
        for task in data["task_instances"]
        if task["state"] == "failed"
    ]


def main():

    run = get_latest_run()

    if run is None:
        print("DAG chưa có run nào.")
        return

    run_id = run["dag_run_id"]
    state = run["state"]

    print("DAG:", DAG_ID)
    print("Run ID:", run_id)
    print("State:", state)

    if state == "success":
        print("DAG chạy thành công.")

    elif state == "failed":

        print("DAG thất bại.")

        failed_tasks = get_failed_tasks(run_id)

        for task in failed_tasks:
            print("Task lỗi:", task["task_id"])

    else:
        print("DAG chưa hoàn thành.")


if __name__ == "__main__":
    main()