import sys
import requests

AIRFLOW_BASE_URL = "http://127.0.0.1:8080"
TOKEN = (
    "eyJhbGciOiJIUzUxMiIsImtpZCI6Im5vdC11c2VkIiwidHlwIjoiSldUIn0."
    "eyJzdWIiOiJBbm9ueW1vdXMiLCJyb2xlIjoiQURNSU4iLCJ0ZWFtcyI6W10sImp0aSI6"
    "IjIwYTJiMzY1YjM0YzQzNDhiOGMxZTE2MWI1Nzg4ZTAzIiwiYXVkIjoiYXBhY2hlLWFpcmZs"
    "b3ciLCJuYmYiOjE3ODg0MzA0NDQsImV4cCI6MTc4ODUxNjg0NCwiaWF0IjoxNzg4NDMwNDQ0"
    "fQ.kD3EEWqilBqcOq0OX8hRE8BRd5PVkVFLQydhmYdFgja7-EwpXN-GFKaq_UyZOK4OmcfUK"
    "n3rTap5jhcZsXNldg"
)
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
TIMEOUT = 10  # giây, tránh script treo vô hạn nếu Airflow không phản hồi


class AirflowApiError(Exception):
    """Lỗi tùy chỉnh để phân biệt lỗi gọi API Airflow với lỗi khác."""


def _call_api(url: str, params: dict | None = None) -> dict:
    """
    Gọi GET tới Airflow API, bắt lỗi chi tiết theo từng loại nguyên nhân.
    Trả về dict JSON nếu thành công, raise AirflowApiError nếu thất bại.
    """
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=TIMEOUT)
    except requests.exceptions.ConnectionError as e:
        raise AirflowApiError(
            f"Không kết nối được tới Airflow tại {AIRFLOW_BASE_URL} — "
            f"kiểm tra webserver đã chạy chưa. Chi tiết: {e}"
        ) from e
    except requests.exceptions.Timeout as e:
        raise AirflowApiError(f"Timeout sau {TIMEOUT}s khi gọi {url}. Chi tiết: {e}") from e
    except requests.exceptions.RequestException as e:
        raise AirflowApiError(f"Lỗi request không xác định khi gọi {url}. Chi tiết: {e}") from e

    if resp.status_code == 401:
        raise AirflowApiError("Token hết hạn hoặc không hợp lệ (401 Unauthorized).")
    if resp.status_code == 404:
        raise AirflowApiError(f"Không tìm thấy resource (404): {url}")
    if resp.status_code != 200:
        raise AirflowApiError(
            f"Airflow API trả về lỗi {resp.status_code} cho {url}: {resp.text[:300]}"
        )

    try:
        return resp.json()
    except ValueError as e:
        raise AirflowApiError(f"Response không phải JSON hợp lệ từ {url}. Chi tiết: {e}") from e


def get_latest_run(dag_id: str) -> dict | None:
    """Lấy dag_run mới nhất theo logical_date. Trả về None nếu chưa có run nào."""
    url = f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns"
    params = {"order_by": "-logical_date", "limit": 1}

    try:
        data = _call_api(url, params=params)
    except AirflowApiError as e:
        print(f"[get_latest_run] {e}")
        return None

    try:
        runs = data["dag_runs"]
    except KeyError:
        print(f"[get_latest_run] Response thiếu key 'dag_runs': {data}")
        return None

    if not runs:
        print(f"[get_latest_run] DAG '{dag_id}' chưa có lần chạy nào.")
        return None

    return runs[0]


def get_failed_tasks(dag_id: str, run_id: str) -> list[dict]:
    """Lấy danh sách task instance có state == 'failed' trong 1 dag_run."""
    url = f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances"

    try:
        data = _call_api(url)
    except AirflowApiError as e:
        print(f"[get_failed_tasks] {e}")
        return []

    try:
        task_instances = data["task_instances"]
    except KeyError:
        print(f"[get_failed_tasks] Response thiếu key 'task_instances': {data}")
        return []

    return [t for t in task_instances if t.get("state") == "failed"]


def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str | None:
    """
    Lấy nội dung log của 1 task instance đã fail, để biết chính xác exception gì.
    try_number: nếu task có retry, đổi số này để xem log của lần thử tương ứng.
    """
    url = (
        f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
        f"/taskInstances/{task_id}/logs/{try_number}"
    )

    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    except requests.exceptions.RequestException as e:
        print(f"[get_task_log] Lỗi khi lấy log của task '{task_id}': {e}")
        return None

    if resp.status_code != 200:
        print(f"[get_task_log] Không lấy được log ({resp.status_code}) cho task '{task_id}'")
        return None

    return resp.text


def main():
    dag_id = "real_estate_eda_pipeline"

    run = get_latest_run(dag_id)
    if run is None:
        print("Không tìm thấy DAG run.")
        sys.exit(1)

    try:
        run_id = run["dag_run_id"]
        state = run["state"]
    except KeyError as e:
        print(f"[main] DAG run thiếu field bắt buộc: {e}")
        sys.exit(1)

    print("Run ID:", run_id)
    print("State:", state)

    if state == "success":
        print("DAG chạy thành công.")
        return

    if state == "failed":
        print("DAG chạy thất bại. Đang lấy danh sách task lỗi...")
        failed_tasks = get_failed_tasks(dag_id, run_id)

        if not failed_tasks:
            print("Không lấy được task bị lỗi (có thể do lỗi API, xem log phía trên).")
            return

        for task in failed_tasks:
            task_id = task.get("task_id", "<unknown>")
            print(f"\n--- Task lỗi: {task_id} ---")

            log_text = get_task_log(dag_id, run_id, task_id)
            if log_text:
                print(log_text[-1500:])
            else:
                print("Không lấy được log chi tiết cho task này.")
    else:
        print(f"DAG đang ở trạng thái '{state}' (chưa kết thúc hoặc trạng thái khác).")


if __name__ == "__main__":
    main()