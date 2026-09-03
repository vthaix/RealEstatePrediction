from airflow.sdk import dag, task
from pendulum import datetime

import requests
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import os


@dag(
    dag_id="realestate_eda"
)
def realestate_eda():

    @task
    def load_data():
        df = pd.read_csv(
            "/usr/local/airflow/include/vietnam_real_estate_fnl.csv"
        )

        numeric_cols = [
            "price",
            "area",
            "floor_count",
            "frontage_width",
            "bedroom_count",
            "bathroom_count"
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df[
            (df["price"] > 0) &
            (df["area"] > 0)
        ].copy()

        df["price_per_m2"] = df["price"] / df["area"]

        return df.to_json()


    @task
    def eda_area_price(data):
        df = pd.read_json(data)

        # =========================
        # Giá theo nhóm diện tích
        # =========================

        df["area_group"] = pd.cut(
            df["area"],
            bins=[0, 50, 80, 120, 200, np.inf],
            labels=["≤50", "51-80", "81-120", "121-200", ">200"]
        )

        plt.figure(figsize=(10, 6))

        sns.boxplot(
            data=df,
            x="area_group",
            y="price_per_m2"
        )

        plt.title("Phân bố giá/m2 theo nhóm diện tích")
        plt.xlabel("Nhóm diện tích (m2)")
        plt.ylabel("Giá / m2")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/01_area_price.png"
        )

        plt.close()


    @task
    def eda_area_price_m2(data):
        df = pd.read_json(data)

        # =========================
        # Diện tích và giá/m2
        # =========================

        plt.figure(figsize=(10, 6))

        sns.scatterplot(
            data=df,
            x="area",
            y="price_per_m2",
            alpha=0.5
        )

        plt.title("Mối quan hệ giữa diện tích và giá/m2")
        plt.xlabel("Diện tích (m2)")
        plt.ylabel("Giá / m2")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/02_area_price_m2.png"
        )

        plt.close()


    @task
    def eda_province(data):
        df = pd.read_json(data)

        # =========================
        # Giá theo tỉnh/thành phố
        # =========================

        province_stats = (
            df.groupby("province_name")
              .agg(
                  median_price=("price", "median"),
                  median_price_per_m2=("price_per_m2", "median"),
                  count=("price", "count")
              )
              .sort_values(
                  "median_price_per_m2",
                  ascending=False
              )
        )

        province_counts = df["province_name"].value_counts()

        valid_provinces = province_counts[
            province_counts >= 30
        ].index

        df_province = df[
            df["province_name"].isin(valid_provinces)
        ].copy()

        province_median = (
            df_province
            .groupby("province_name")["price_per_m2"]
            .median()
            .sort_values(ascending=False)
            .reset_index()
        )

        plt.figure(figsize=(12, 8))

        sns.barplot(
            data=province_median,
            x="price_per_m2",
            y="province_name"
        )

        plt.title("Giá/m2 trung vị theo tỉnh/thành phố")
        plt.xlabel("Giá/m2 trung vị")
        plt.ylabel("Tỉnh/Thành phố")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/03_province.png"
        )

        plt.close()


    @task
    def eda_house_type(data):
        df = pd.read_json(data)

        # =========================
        # Giá theo loại nhà
        # =========================

        house_type_stats = (
            df.groupby("property_type_name")
              .agg(
                  median_price=("price", "median"),
                  median_price_per_m2=("price_per_m2", "median"),
                  count=("price", "count")
              )
              .sort_values(
                  "median_price",
                  ascending=False
              )
        )

        # Chỉ lấy loại nhà có ít nhất 30 BĐS
        valid_types = (
            house_type_stats[
                house_type_stats["count"] >= 30
            ]
            .index
        )

        df_house = df[
            df["property_type_name"].isin(valid_types)
        ].copy()

        plt.figure(figsize=(12, 8))

        sns.boxplot(
            data=df_house,
            x="price_per_m2",
            y="property_type_name"
        )

        plt.title("Phân bố giá/m2 theo loại nhà")
        plt.xlabel("Giá / m2")
        plt.ylabel("Loại nhà")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/04_house_type.png"
        )

        plt.close()


    @task
    def eda_bed_bath_floor(data):
        df = pd.read_json(data)

        # =========================
        # Phòng ngủ - phòng tắm - số tầng
        # =========================

        fig, axes = plt.subplots(
            1,
            3,
            figsize=(18, 6)
        )

        features = [
            ("bedroom_count", "Số phòng ngủ"),
            ("bathroom_count", "Số phòng tắm"),
            ("floor_count", "Số tầng")
        ]

        for ax, (col, title) in zip(axes, features):

            sns.boxplot(
                data=df,
                x=col,
                y="price",
                ax=ax
            )

            ax.set_title(
                f"Giá theo {title.lower()}"
            )

            ax.set_xlabel(title)
            ax.set_ylabel("Giá")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/05_bed_bath_floor.png"
        )

        plt.close()


    @task
    def eda_frontage(data):
        df = pd.read_json(data)

        # =========================
        # Mặt tiền và giá
        # =========================

        plt.figure(figsize=(10, 6))

        sns.scatterplot(
            data=df,
            x="frontage_width",
            y="price",
            alpha=0.5
        )

        sns.regplot(
            data=df,
            x="frontage_width",
            y="price",
            scatter=False
        )

        plt.title(
            "Mối quan hệ giữa chiều rộng mặt tiền và giá"
        )

        plt.xlabel(
            "Chiều rộng mặt tiền (m)"
        )

        plt.ylabel("Giá")

        plt.tight_layout()

        os.makedirs(
            "/usr/local/airflow/include/output",
            exist_ok=True
        )

        plt.savefig(
            "/usr/local/airflow/include/output/06_frontage.png"
        )

        plt.close()


    # =========================
    # DAG FLOW
    # =========================

    data = load_data()

    eda_area_price(data)
    eda_area_price_m2(data)
    eda_province(data)
    eda_house_type(data)
    eda_bed_bath_floor(data)
    eda_frontage(data)


realestate_eda()