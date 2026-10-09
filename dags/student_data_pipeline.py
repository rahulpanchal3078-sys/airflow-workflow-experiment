from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import csv

INPUT_FILE = "/opt/airflow/data/students.csv"
OUTPUT_FILE = "/opt/airflow/data/student_results.csv"

def run_start_pipeline():
    print("Starting Airflow Student Data Pipeline")
    return "Pipeline started"

def run_extract_data(**context):
    students = []
    with open(INPUT_FILE, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            students.append({
                "student_id": row["student_id"],
                "name": row["name"],
                "subject": row["subject"],
                "marks": int(row["marks"])
            })
    print(f"Extracted {len(students)} student records")
    return students

def run_transform_data(**context):
    students = context['ti'].xcom_pull(task_ids='extract_data')
    transformed = []
    for s in students:
        m = s["marks"]
        if m >= 75:
            grade = "Distinction"
        elif m >= 60:
            grade = "First Class"
        elif m >= 40:
            grade = "Pass"
        else:
            grade = "Fail"
        s["grade"] = grade
        transformed.append(s)
    print("Student marks transformed successfully")
    return transformed

def run_load_data(**context):
    students = context['ti'].xcom_pull(task_ids='transform_data')
    fieldnames = ["student_id", "name", "subject", "marks", "grade"]
    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(students)
    print(f"Processed results saved to {OUTPUT_FILE}")
    return OUTPUT_FILE

def run_pipeline_summary(**context):
    students = context['ti'].xcom_pull(task_ids='transform_data')
    total = len(students)
    passed = len([s for s in students if s["marks"] >= 40])
    failed = total - passed
    average = sum(s["marks"] for s in students) / total
    print("---------- PIPELINE SUMMARY ----------")
    print(f"Total Students : {total}")
    print(f"Passed         : {passed}")
    print(f"Failed         : {failed}")
    print(f"Average Marks  : {average:.2f}")
    print("---------------------------------------")

with DAG(
    dag_id="university_student_data_pipeline",
    description="University experiment for designing, scheduling and monitoring workflows",
    schedule="0 9 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["university", "data-pipeline", "experiment"],
) as dag:

    t_start = PythonOperator(
        task_id="start_pipeline",
        python_callable=run_start_pipeline
    )

    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=run_extract_data
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform_data
    )

    t_load = PythonOperator(
        task_id="load_data",
        python_callable=run_load_data
    )

    t_summary = PythonOperator(
        task_id="pipeline_summary",
        python_callable=run_pipeline_summary
    )

    t_start >> t_extract >> t_transform >> t_load >> t_summary