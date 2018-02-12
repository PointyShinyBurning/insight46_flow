from airflow import DAG
from cpgintegrate.connectors import OpenClinica, XNAT
from cpgintegrate.airflow.dag_maker import dataset_list_to_ckan
from datetime import datetime
# from cpgintegrate.airflow.cpg_airflow_plugin import CPGDatasetToXCom, XComDatasetToCkan
from airflow.operators.cpg_plugin import CPGDatasetToXCom, XComDatasetToCkan

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['d.key@ucl.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
}

insight = DAG('insight46_flow', start_date=datetime(2018, 2, 11), schedule_interval='1 0 * * *',
              default_args=default_args)

dataset_list_to_ckan(insight, OpenClinica, 'insight46_openclinica', 'ckan', 'insight46_admin', 'openclinica',
                     dataset_list=[
                         'F_3DECHOANALYS',
                         'F_CIMTDONE',
                         'F_ECGSABREV3',
                         'F_ECHODONE',
                         'F_FINGERTAPPIN',
                         'F_HEARTRHYTHM',
                         'F_IMAGEQUALITY',
                         'F_INCIDENTALFI_2099',
                         'F_OXYPROJ',
                         'F_SPHYGMACOR',
                         'F_SPOONS',
                         'F_STROOP',
                         'F_TRAILMAKING',
                         'F_URINECOLLECT',
                         'F_VALVEDISORDE',
                         'F_VICORDERFILE'
                     ])
with insight as dag:
    CPGDatasetToXCom(task_id='XNAT_SESSIONS', connector_class=XNAT, connection_id="insight46_xnat")\
        >> XComDatasetToCkan(task_id='XNAT_SESSIONS_push_to_ckan', ckan_connection_id='ckan', ckan_package_id='insight46_admin')