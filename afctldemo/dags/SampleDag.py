# Importing Qubole Operator in DAG

import airflow

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.qubole_operator import QuboleOperator
from datetime import timedelta

# these args will get passed on to each operator

# you can override them on a per-task basis during operator initialization

default_args = {
  'owner': 'vsharma',
  'depends_on_past': False,
  'start_date': airflow.utils.dates.days_ago(1),
  'email': ['vsharma@qubole.com'],
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}

prog = '''
import scala.math.random

import org.apache.spark._

/** Computes an approximation to pi */
object SparkPi {
def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.parallelize(1 until n, slices).map { i =>
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
    }
}
'''

dag = DAG(
  'SampleDag',
  default_args=default_args,
  description='A simple tutorial DAG jmsample',
  schedule_interval=timedelta(days=1))

# Spark Command - Scala Program
QuboleOperator(
    task_id='spark_cmd',
    command_type="sparkcmd",
    program=prog,
    language='scala',
    arguments='--class SparkPi',
    cluster_label='spark-baseline',
    qubole_conn_id='qubole_default',
    dag=dag)