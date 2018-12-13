import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}

dag = DAG(
    dag_id='fluxo_simples',  # id unico
    default_args=args,  # argumentos padrao e refletidos nos nós do grafo
    schedule_interval=timedelta(days=1),  # define a periodicidade de execução
    dagrun_timeout=timedelta(minutes=60)  # tempo de execuçao permitida para a DAG antes de interrompela

)

# Imprimir a data na saida padrao
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

# dorme por 5 segundos
# repete 3 vezes em caso de erro
task2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

# salvar a data em um arquivo de texto
# repete 2 vezes em caso de erro
task3 = BashOperator(
    task_id='save_date',
    bash_command='date > /tmp/date_output.txt',
    retries=2,
    dag=dag
)

# Aqui estou definindo a interligação entre as tasks
# Task1 aa task2
task1.set_downstream(task2)
#Task2 aa task3
task2.set_downstream(task3)