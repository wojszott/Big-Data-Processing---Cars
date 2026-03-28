from datetime import datetime
from airflow import DAG
from airflow.sdk import Param  
from airflow.providers.standard.operators.bash import BashOperator  
from airflow.providers.standard.operators.python import BranchPythonOperator  

with DAG(
    dag_id="project1-workflow",
    start_date=datetime(2015, 12, 1),
    schedule=None,
        params={
            "dags_home": Param(
                "TU UZUPELNIJ SCIEZKE DLA TWOJEGO KATALOGU AIRFLOW/airflow/dags", type="string"
            ),
            "input_dir": Param(
                "TU UZUPELNIJ SCIEZKE DLA TWOICH DANYCH ZRODLOWYCH/input", type="string"
            ),
            "output_mr_dir": Param("/project1/output_mr3", type="string"),
            "output_dir": Param("/project1/output6", type="string"),
            "classic_or_streaming": Param(
                "streaming", enum=["classic", "streaming"]
            ),
        },
    render_template_as_native_obj=True,
    catchup=False,  
) as dag:

    # Usuwanie katalogów z HDFS jeśli istnieją
    clean_output_mr_dir = BashOperator(
        task_id="clean_output_mr_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_mr_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_mr_dir }}; fi"
        ),
    )

    clean_output_dir = BashOperator(
        task_id="clean_output_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_dir }}; fi"
        ),
    )

    # Wybór trybu wykonania: klasyczny MR lub streaming
    def _pick_classic_or_streaming(params):
        if params["classic_or_streaming"] == "classic":
            return "mapreduce_classic"
        else:
            return "hadoop_streaming"

    pick_classic_or_streaming = BranchPythonOperator(
        task_id="pick_classic_or_streaming",
        python_callable=_pick_classic_or_streaming,
        op_kwargs={"params": dag.params},
    )

    # MapReduce klasyczny
    mapreduce_classic = BashOperator(
        task_id="mapreduce_classic",
        bash_command=(
            "hadoop jar {{ params.dags_home }}/project_files/ . . . "
        ),
    )

    # MapReduce streaming
    hadoop_streaming = BashOperator(
        task_id="hadoop_streaming",
        bash_command=(
            "INPUT_DIR={{ params.input_dir }}/datasource1 "
            "OUTPUT_DIR={{ params.output_mr_dir }} && "
            "hdfs dfs -rm -r -f $OUTPUT_DIR || true && "
            "hadoop jar /usr/lib/hadoop/hadoop-streaming.jar "
            "-input $INPUT_DIR "
            "-output $OUTPUT_DIR "
            "-mapper '{{ params.dags_home }}/project_files/mapper.py' "
            "-combiner '{{ params.dags_home }}/project_files/combiner.py' "
            "-reducer '{{ params.dags_home }}/project_files/reducer.py' "
            "-file '{{ params.dags_home }}/project_files/mapper.py' "
            "-file '{{ params.dags_home }}/project_files/combiner.py' "
            "-file '{{ params.dags_home }}/project_files/reducer.py'"
        ),
    )

    # Program Hive
    hive = BashOperator(
        task_id="hive",
        bash_command=(
            "INPUT_DIR3={{ params.output_mr_dir }} "
            "INPUT_DIR4={{ params.input_dir }}/datasource4 "
            "OUTPUT_DIR6={{ params.output_dir }} && "
            "beeline -u jdbc:hive2://localhost:10000/default "
            "--silent=true --outputformat=csv2 "
            "--hiveconf input_dir3=$INPUT_DIR3 "
            "--hiveconf input_dir4=$INPUT_DIR4 "
            "--hiveconf output_dir6=$OUTPUT_DIR6 "
            "--hiveconf fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem "
            "--hiveconf fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS "
            "-f '{{ params.dags_home }}/project_files/hive.hql'"
        ),
        trigger_rule="none_failed",
    )

    # Pobranie wyników
    get_output = BashOperator(
        task_id="get_output",
        bash_command=(
            "hadoop fs -getmerge {{ params.output_dir }} output6.json && head output6.json"
        ),
        trigger_rule="none_failed",
    )

    # Zależności
    [clean_output_mr_dir, clean_output_dir] >> pick_classic_or_streaming
    pick_classic_or_streaming >> [mapreduce_classic, hadoop_streaming]
    [mapreduce_classic, hadoop_streaming] >> hive
    hive >> get_output
