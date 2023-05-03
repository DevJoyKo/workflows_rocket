from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='wiki_page_views',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly"
)

# get_data = BashOperator(
#     task_id="get_data",
#     bash_command=(
#         "curl -o /tmp/wikipageviews.gz "
#         "https://dumps.wikimedia.org/other/pageviews/"
#         "{{execution_date.year}}"
#         "{{execution_date.year}}-"
#         "{{ '{:02}'.format(execution_date.month) }}/"
#         "pageviews-{{execution_date.year}}"
#         "{{ '{:02}'.format(execution_date.month) }}"
#         "{{ '{:02}'.format(execution_date.day) }}-"
#         "{{ '{:02}'.format(execution_date.hour }}0000.gz"
#     ),
#     dag=dag,
# )

def _print_context(**context):
    # print(context)
    start=context["execution_date"]
    end=context["next_execution_date"]
    print(f'start: {start}, end: {end}')

def _get_data(year, month, day, hour, output_path, **context):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/"
        f"pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    print(url)
    request.urlretrieve(url, output_path)

get_data=PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
            "year": "{{execution_date.year}}",
            "month": "{{execution_date.month}}",
            "day": "{{execution_date.day}}",
            "hour": "{{execution_date.hour}}",
            "output_path": "/tmp/wikipageviews.gz",
        },
    dag=dag,
)

print_context=PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
)

get_data >> print_context