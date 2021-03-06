from datetime import datetime

from airflow import DAG
from airflow.models import Variable
# from cpgintegrate.airflow.cpg_airflow_plugin import CPGDatasetToXCom, XComDatasetToCkan
from airflow.operators.cpg_plugin import CPGDatasetToXCom, XComDatasetToCkan
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from cpgintegrate.airflow.subdags import dataset_list_subdag
from cpgintegrate.connectors.openclinica import OpenClinica
from cpgintegrate.connectors.xnat import XNAT

START_DATE = datetime(2018, 2, 11)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [Variable.get('alert_email', None)],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': START_DATE,
}


insight = DAG('insight46_flow', default_args=default_args, schedule_interval='0 21 * * *')
with insight as dag:
    oc_args = {'connector_class': OpenClinica, 'connection_id': 'openclinica', 'pool': 'openclinica',
               'connector_kwargs': {'schema': 'S_INSIGHT4'}}
    DummyOperator(task_id="starter") >> [SubDagOperator(task_id="AllOpenClinica",
                   subdag=dataset_list_subdag(connector_kwargs={'schema': 'S_INSIGHT4'}, pool='openclinica',
                       dag_id="insight46_flow.AllOpenClinica", connector_class=OpenClinica,
                         connection_id='openclinica',
                         ckan_connection_id='ckan', ckan_package_id='insight46_admin',
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
                         ], start_date=START_DATE)), (
            XComDatasetToCkan(task_id='XNAT_SESSIONS_push_to_ckan', ckan_connection_id='ckan',
                              ckan_package_id='insight46_admin') <<
            CPGDatasetToXCom(task_id='XNAT_SESSIONS', connector_class=XNAT, connection_id="xnat",
                             connector_kwargs={'schema': 'INSIGHT46_2'})

    ),(
             XComDatasetToCkan(task_id='Spoons_push_to_ckan',
                               ckan_connection_id='ckan',
                               ckan_package_id='insight46_iom') <<
             CPGDatasetToXCom(task_id='Spoons', **oc_args, dataset_args=['F_SPOONS'])
    ),
    ]
