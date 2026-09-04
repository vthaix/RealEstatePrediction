"""
DAG: real_estate_eda_pipeline (TaskFlow API - airflow.sdk)
------------------------------------------------------------
Pipeline EDA (Exploratory Data Analysis) cho dữ liệu tin đăng bất động sản,
viết theo TaskFlow API mới của Airflow 3.x (`from airflow.sdk import dag, task`).

Input : /usr/local/airflow/include/vietnam_real_estate_fnl.csv
         (đổi qua Airflow Variable "real_estate_raw_path")

Output: /usr/local/airflow/include/output/
    ├── cleaned_data.csv
    ├── filtered_data.csv
    ├── price_analysis.csv
    ├── area_analysis.csv
    ├── location_analysis.csv
    ├── correlation_matrix.csv
    ├── eda_summary.json
    ├── charts/
    │   ├── price_distribution.png
    │   ├── price_by_district.png
    │   └── area_vs_price.png
    └── report.html

Schema input (20 cột):
    Unnamed: 0, name, description, property_type_name, province_name,
    district_name, ward_name, street_name, project_name, price, area,
    floor_count, frontage_width, house_depth, road_width, bedroom_count,
    bathroom_count, house_direction, balcony_direction, published_at
------------------------------------------------------------
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

import pandas as pd
import matplotlib

matplotlib.use("Agg")  # không cần GUI, chạy được trong worker Airflow
import matplotlib.pyplot as plt
import seaborn as sns

from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

RAW_DATA_PATH = Variable.get(
    "real_estate_raw_path",
    default_var="/usr/local/airflow/include/vietnam_real_estate_fnl1.csv",
)
OUTPUT_DIR = Variable.get(
    "real_estate_output_dir", default_var="/usr/local/airflow/include/output"
)
CHARTS_DIR = os.path.join(OUTPUT_DIR, "charts")

NUMERIC_COLS = [
    "price",
    "area",
    "floor_count",
    "frontage_width",
    "house_depth",
    "road_width",
    "bedroom_count",
    "bathroom_count",
]

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def _ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHARTS_DIR, exist_ok=True)

@dag(
    dag_id="real_estate_eda_pipeline",
    description="Pipeline làm sạch & EDA dữ liệu tin đăng bất động sản (TaskFlow API)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["real_estate", "eda", "pandas", "taskflow"],
)
def real_estate_eda_pipeline():

    @task
    def clean_data() -> dict:
        _ensure_dirs()

        if not os.path.exists(RAW_DATA_PATH):
            raise AirflowFailException(
                f"Không tìm thấy file input tại '{RAW_DATA_PATH}'. "
                f"Kiểm tra lại đường dẫn hoặc Airflow Variable 'real_estate_raw_path'."
            )

        try:
            df = pd.read_csv(RAW_DATA_PATH)
        except pd.errors.EmptyDataError as e:
            raise AirflowFailException(f"File '{RAW_DATA_PATH}' rỗng, không có dữ liệu để đọc.") from e
        except pd.errors.ParserError as e:
            raise AirflowFailException(
                f"File '{RAW_DATA_PATH}' không đúng định dạng CSV, không parse được: {e}"
            ) from e
        except (OSError, UnicodeDecodeError) as e:
            raise AirflowFailException(f"Lỗi khi đọc file '{RAW_DATA_PATH}': {e}") from e

        if df.empty:
            raise AirflowFailException(f"File '{RAW_DATA_PATH}' đọc được nhưng không có dòng nào.")

        missing_cols = [c for c in ["name", "area", "price"] if c not in df.columns]
        if missing_cols:
            raise AirflowFailException(
                f"File input thiếu cột bắt buộc: {missing_cols}. "
                f"Các cột hiện có: {list(df.columns)}"
            )

        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        for col in NUMERIC_COLS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "published_at" in df.columns:
            df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

        obj_cols = df.select_dtypes(include="object").columns
        for col in obj_cols:
            df[col] = df[col].astype(str).str.strip()

        df = df.drop_duplicates()
        df = df.dropna(subset=["name", "area"])

        out_path = os.path.join(OUTPUT_DIR, "cleaned_data.csv")
        df.to_csv(out_path, index=False)
        print(f"[clean_data] Đã lưu {len(df)} dòng -> {out_path}")
        return {"path": out_path, "rows": len(df)}
    
    @task
    def filter_data(cleaned: dict) -> dict:
        df = pd.read_csv(cleaned["path"])
        before = len(df)

        df = df[(df["price"].notna()) & (df["price"] > 0)]
        df = df[(df["area"].notna()) & (df["area"] > 0)]

        for col in ["price", "area"]:
            q1, q3 = df[col].quantile([0.01, 0.99])
            df = df[(df[col] >= q1) & (df[col] <= q3)]

        df = df[(df["area"] >= 10) & (df["area"] <= 5000)]

        after = len(df)
        out_path = os.path.join(OUTPUT_DIR, "filtered_data.csv")
        df.to_csv(out_path, index=False)
        print(f"[filter_data] {before} -> {after} dòng sau khi lọc -> {out_path}")
        return {"path": out_path, "rows": after}

    @task
    def price_analysis(filtered: dict) -> str:
        df = pd.read_csv(filtered["path"])
        df["price_per_m2"] = df["price"] / df["area"]

        overall = pd.DataFrame(
            [
                {
                    "group": "ALL",
                    "count": df["price"].count(),
                    "mean_price": df["price"].mean(),
                    "median_price": df["price"].median(),
                    "std_price": df["price"].std(),
                    "min_price": df["price"].min(),
                    "max_price": df["price"].max(),
                    "mean_price_per_m2": df["price_per_m2"].mean(),
                    "median_price_per_m2": df["price_per_m2"].median(),
                }
            ]
        )

        by_type = (
            df.groupby("property_type_name")
            .agg(
                count=("price", "count"),
                mean_price=("price", "mean"),
                median_price=("price", "median"),
                mean_price_per_m2=("price_per_m2", "mean"),
            )
            .reset_index()
            .rename(columns={"property_type_name": "group"})
        )
        by_type.insert(0, "dimension", "property_type_name")
        overall.insert(0, "dimension", "overall")

        result = pd.concat([overall, by_type], ignore_index=True)
        out_path = os.path.join(OUTPUT_DIR, "price_analysis.csv")
        result.to_csv(out_path, index=False)
        print(f"[price_analysis] Đã lưu -> {out_path}")
        return out_path

    @task
    def area_analysis(filtered: dict) -> str:
        df = pd.read_csv(filtered["path"])

        bins = [0, 30, 50, 80, 120, 200, 5000]
        labels = ["<30m2", "30-50m2", "50-80m2", "80-120m2", "120-200m2", ">200m2"]
        df["area_group"] = pd.cut(df["area"], bins=bins, labels=labels)

        stats = pd.DataFrame(
            [
                {"metric": "mean_area", "value": df["area"].mean()},
                {"metric": "median_area", "value": df["area"].median()},
                {"metric": "std_area", "value": df["area"].std()},
                {"metric": "min_area", "value": df["area"].min()},
                {"metric": "max_area", "value": df["area"].max()},
            ]
        )

        by_group = (
            df.groupby("area_group", observed=True)
            .agg(count=("area", "count"), mean_price=("price", "mean"))
            .reset_index()
            .rename(columns={"area_group": "group"})
        )
        by_group.insert(0, "dimension", "area_group")
        stats.insert(0, "dimension", "summary_stats")
        stats = stats.rename(columns={"metric": "group", "value": "mean_price"})

        result = pd.concat([stats, by_group], ignore_index=True)
        out_path = os.path.join(OUTPUT_DIR, "area_analysis.csv")
        result.to_csv(out_path, index=False)
        print(f"[area_analysis] Đã lưu -> {out_path}")
        return out_path

    @task
    def location_analysis(filtered: dict) -> str:
        df = pd.read_csv(filtered["path"])
        df["price_per_m2"] = df["price"] / df["area"]

        by_province = (
            df.groupby("province_name")
            .agg(
                count=("price", "count"),
                mean_price=("price", "mean"),
                mean_price_per_m2=("price_per_m2", "mean"),
                mean_area=("area", "mean"),
            )
            .reset_index()
            .rename(columns={"province_name": "location"})
        )
        by_province.insert(0, "dimension", "province_name")

        by_district = (
            df.groupby(["province_name", "district_name"])
            .agg(
                count=("price", "count"),
                mean_price=("price", "mean"),
                mean_price_per_m2=("price_per_m2", "mean"),
                mean_area=("area", "mean"),
            )
            .reset_index()
        )
        by_district["location"] = (
            by_district["province_name"] + " - " + by_district["district_name"].astype(str)
        )
        by_district = by_district.drop(columns=["province_name", "district_name"])
        by_district.insert(0, "dimension", "district_name")

        result = pd.concat([by_province, by_district], ignore_index=True)
        out_path = os.path.join(OUTPUT_DIR, "location_analysis.csv")
        result.to_csv(out_path, index=False)
        print(f"[location_analysis] Đã lưu -> {out_path}")
        return out_path

    @task
    def correlation_matrix(filtered: dict) -> str:
        df = pd.read_csv(filtered["path"])
        cols = [c for c in NUMERIC_COLS if c in df.columns]
        corr = df[cols].corr(numeric_only=True)
        out_path = os.path.join(OUTPUT_DIR, "correlation_matrix.csv")
        corr.to_csv(out_path)
        print(f"[correlation_matrix] Đã lưu -> {out_path}")
        return out_path
    
    @task
    def eda_summary(cleaned: dict, filtered: dict, *_deps) -> str:
        df = pd.read_csv(filtered["path"])
        df["price_per_m2"] = df["price"] / df["area"]

        missing_pct = pd.read_csv(RAW_DATA_PATH).isna().mean().mul(100).round(2).to_dict()

        summary = {
            "generated_at": datetime.now().isoformat(),
            "cleaned_rows": cleaned["rows"],
            "filtered_rows": filtered["rows"],
            "missing_pct_raw": missing_pct,
            "price": {
                "mean": float(df["price"].mean()),
                "median": float(df["price"].median()),
                "std": float(df["price"].std()),
                "min": float(df["price"].min()),
                "max": float(df["price"].max()),
            },
            "area": {
                "mean": float(df["area"].mean()),
                "median": float(df["area"].median()),
                "std": float(df["area"].std()),
                "min": float(df["area"].min()),
                "max": float(df["area"].max()),
            },
            "price_per_m2": {
                "mean": float(df["price_per_m2"].mean()),
                "median": float(df["price_per_m2"].median()),
            },
            "top_provinces": df["province_name"].value_counts().head(5).to_dict(),
            "top_property_types": df["property_type_name"]
            .value_counts()
            .head(5)
            .to_dict(),
        }

        out_path = os.path.join(OUTPUT_DIR, "eda_summary.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)
        print(f"[eda_summary] Đã lưu -> {out_path}")
        return out_path

    @task
    def generate_charts(filtered: dict, _summary_path: str) -> str:
        _ensure_dirs()
        df = pd.read_csv(filtered["path"])
        sns.set_theme(style="whitegrid")

        plt.figure(figsize=(9, 5))
        sns.histplot(df["price"], bins=50, kde=True, color="#2E86AB")
        plt.title("Phân phối giá bất động sản")
        plt.xlabel("Giá (VND)")
        plt.ylabel("Số lượng tin")
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "price_distribution.png"), dpi=150)
        plt.close()

        top_districts = (
            df.groupby("district_name")["price"]
            .mean()
            .sort_values(ascending=False)
            .head(15)
            .reset_index()
        )
        plt.figure(figsize=(10, 6))
        sns.barplot(data=top_districts, x="price", y="district_name", color="#F18F01")
        plt.title("Giá trung bình theo Quận/Huyện (Top 15)")
        plt.xlabel("Giá trung bình (VND)")
        plt.ylabel("Quận/Huyện")
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "price_by_district.png"), dpi=150)
        plt.close()

        plt.figure(figsize=(9, 6))
        sns.scatterplot(
            data=df.sample(min(3000, len(df)), random_state=42),
            x="area",
            y="price",
            alpha=0.4,
            color="#C73E1D",
        )
        plt.title("Mối quan hệ Diện tích vs Giá")
        plt.xlabel("Diện tích (m2)")
        plt.ylabel("Giá (VND)")
        plt.tight_layout()
        plt.savefig(os.path.join(CHARTS_DIR, "area_vs_price.png"), dpi=150)
        plt.close()

        print(f"[generate_charts] Đã lưu 3 biểu đồ -> {CHARTS_DIR}")
        return CHARTS_DIR

    @task
    def generate_report(summary_path: str, _charts_dir: str) -> str:
        with open(summary_path, encoding="utf-8") as f:
            summary = json.load(f)

        def fmt(n):
            try:
                return f"{n:,.0f}"
            except (TypeError, ValueError):
                return str(n)

        top_provinces_rows = "".join(
            f"<tr><td>{k}</td><td>{v}</td></tr>"
            for k, v in summary["top_provinces"].items()
        )
        top_types_rows = "".join(
            f"<tr><td>{k}</td><td>{v}</td></tr>"
            for k, v in summary["top_property_types"].items()
        )

        html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<title>Báo cáo EDA - Dữ liệu Bất động sản</title>
<style>
  body {{ font-family: -apple-system, Arial, sans-serif; margin: 40px; color: #222; }}
  h1 {{ color: #2E86AB; }}
  h2 {{ color: #C73E1D; border-bottom: 2px solid #eee; padding-bottom: 6px; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 24px; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}
  th {{ background-color: #2E86AB; color: white; }}
  .metric-grid {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 24px; }}
  .metric-card {{ background: #f7f7f7; border-radius: 8px; padding: 16px 20px; min-width: 180px; }}
  .metric-card .label {{ font-size: 13px; color: #666; }}
  .metric-card .value {{ font-size: 20px; font-weight: bold; color: #2E86AB; }}
  img {{ max-width: 100%; border: 1px solid #eee; border-radius: 6px; margin-bottom: 24px; }}
  .footer {{ color: #999; font-size: 12px; margin-top: 40px; }}
</style>
</head>
<body>
  <h1>Báo cáo EDA - Dữ liệu Bất động sản</h1>
  <p>Thời gian tạo báo cáo: {summary['generated_at']}</p>
  <p>Số dòng sau làm sạch: <b>{summary['cleaned_rows']}</b> — Sau lọc outlier: <b>{summary['filtered_rows']}</b></p>

  <h2>Tổng quan Giá</h2>
  <div class="metric-grid">
    <div class="metric-card"><div class="label">Trung bình</div><div class="value">{fmt(summary['price']['mean'])} đ</div></div>
    <div class="metric-card"><div class="label">Trung vị</div><div class="value">{fmt(summary['price']['median'])} đ</div></div>
    <div class="metric-card"><div class="label">Nhỏ nhất</div><div class="value">{fmt(summary['price']['min'])} đ</div></div>
    <div class="metric-card"><div class="label">Lớn nhất</div><div class="value">{fmt(summary['price']['max'])} đ</div></div>
  </div>

  <h2>Tổng quan Diện tích</h2>
  <div class="metric-grid">
    <div class="metric-card"><div class="label">Trung bình</div><div class="value">{summary['area']['mean']:.1f} m²</div></div>
    <div class="metric-card"><div class="label">Trung vị</div><div class="value">{summary['area']['median']:.1f} m²</div></div>
    <div class="metric-card"><div class="label">Giá/m² trung bình</div><div class="value">{fmt(summary['price_per_m2']['mean'])} đ</div></div>
  </div>

  <h2>Biểu đồ</h2>
  <img src="charts/price_distribution.png" alt="Phân phối giá">
  <img src="charts/price_by_district.png" alt="Giá theo quận huyện">
  <img src="charts/area_vs_price.png" alt="Diện tích vs Giá">

  <h2>Top 5 Tỉnh/Thành phố nhiều tin nhất</h2>
  <table><tr><th>Tỉnh/Thành</th><th>Số tin</th></tr>{top_provinces_rows}</table>

  <h2>Top 5 Loại hình BĐS nhiều tin nhất</h2>
  <table><tr><th>Loại hình</th><th>Số tin</th></tr>{top_types_rows}</table>

  <div class="footer">Sinh tự động bởi Airflow DAG: real_estate_eda_pipeline</div>
</body>
</html>
"""
        out_path = os.path.join(OUTPUT_DIR, "report.html")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[generate_report] Đã lưu -> {out_path}")
        return out_path

    cleaned = clean_data()
    filtered = filter_data(cleaned)

    price_result = price_analysis(filtered)
    area_result = area_analysis(filtered)
    location_result = location_analysis(filtered)
    corr_result = correlation_matrix(filtered)

    summary_path = eda_summary(cleaned, filtered, price_result, area_result, location_result, corr_result)
    charts_dir = generate_charts(filtered, summary_path)
    generate_report(summary_path, charts_dir)


real_estate_eda_pipeline()