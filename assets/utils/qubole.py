import os

import itertools
from qds_sdk.cluster import ClusterInfoV13, Cluster
from qds_sdk.clusterv2 import ClusterCmdLine, ClusterV2, ClusterInfoV2
from qds_sdk.engine import Engine
from qds_sdk.commands import DbImportCommand, HiveCommand
from qds_sdk.dbtaps import DbTap
from qds_sdk.qubole import Qubole
from qds_sdk.resource import Resource

from qubole_root import PROJECT_DIR
from utils.template_loader import TemplateLoader

QUERY_DIR = os.path.join(PROJECT_DIR, 'utils/queries')


class DataStoreNotFoundException(Exception):
    pass


class Notebook(Resource):
    rest_entity_path = 'notebooks'

    @classmethod
    def import_(cls, **kwargs):
        conn = Qubole.agent(version='latest')
        return conn.post('notebooks/import', data=kwargs)


def _create_base_cluster_info(label, config):
    cluster_info = ClusterInfoV13(label=label)
    cluster_info.set_cluster_info(
        disallow_cluster_termination=False
    )
    cluster_info.set_ec2_settings(
        aws_region=config['region_name'],
        aws_availability_zone='Any',
        vpc_id=config['cluster_vpc_id'],
        subnet_id=config['cluster_subnet_id'],
        role_instance_profile=None
    )
    cluster_info.set_spot_instance_settings(
        maximum_bid_price_percentage=None,
        timeout_for_request=None,
        maximum_spot_instance_percentage=None
    )
    cluster_info.set_security_settings(
        encrypted_ephemerals=False
    )
    return cluster_info


def _create_hadoop_cluster_info(config):
    cluster_info = ClusterInfoV2(config['hadoop_cluster_name'])
    cluster_info.set_cluster_info(master_instance_type=config['hadoop_master_instance_type'],slave_instance_type=config['hadoop_slave_instance_type'],min_nodes=1,max_nodes=config['hadoop_max_nodes_count'],slave_request_type='spot')

    cloud_config = Qubole.get_cloud(cloud_name='aws')
    cloud_config.set_cloud_config(aws_region=config['region_name'],aws_availability_zone='Any',vpc_id=config['cluster_vpc_id'],subnet_id=config['cluster_subnet_id'])

    engine_config = Engine(flavour='hadoop2')
    engine_config.set_engine_config()

    cluster_request = ClusterCmdLine.get_cluster_request_parameters(cluster_info, cloud_config, engine_config)

    return cluster_request



def _create_spark_cluster_info(config):
    cluster_info = ClusterInfoV2(config['spark_cluster_name'])
    cluster_info.set_cluster_info(master_instance_type=config['hadoop_master_instance_type'],slave_instance_type=config['hadoop_slave_instance_type'],min_nodes=1,max_nodes=config['hadoop_max_nodes_count'],slave_request_type='spot')

    cloud_config = Qubole.get_cloud(cloud_name='aws')
    cloud_config.set_cloud_config(aws_region=config['region_name'],aws_availability_zone='Any',vpc_id=config['cluster_vpc_id'],subnet_id=config['cluster_subnet_id'])

    engine_config = Engine(flavour='spark')
    engine_config.set_engine_config(spark_version='2.1.0')

    cluster_request = ClusterCmdLine.get_cluster_request_parameters(cluster_info, cloud_config, engine_config)
    return cluster_request


def _create_cluster(config, make_cluster_info_fun):
    cluster_request = make_cluster_info_fun(config)

    clusterv2 = ClusterV2()
    response = clusterv2.create(cluster_request)
    return response

def create_hadoop_cluster(config):
    return _create_cluster(config, _create_hadoop_cluster_info)


def create_spark_cluster(config):
    return _create_cluster(config, _create_spark_cluster_info)


def _import_notebook(name, location, url, note_type, cluster_id):
    return Notebook.import_(
        name=name,
        location=location,
        url=url,
        note_type=note_type,
        cluster_id=cluster_id
    )


def import_dashboard_notebook(config, spark_cluster_id):
    return _import_notebook(
        name=config['spark_dashboard_notebook_name'],
        location='Common',
        url=config['spark_dashboard_notebook_s3_url'],
        note_type='spark',
        cluster_id=spark_cluster_id
    )


def import_spark_notebook(config, spark_cluster_id):
    return _import_notebook(
        name=config['spark_notebook_name'],
        location='Common',
        url=config['spark_notebook_s3_url'],
        note_type='spark',
        cluster_id=spark_cluster_id
    )


def import_data_table(data_store_id, source_table_name, database_name):
    return DbImportCommand.create(
        mode=1,
	use_customer_cluster=True,
        customer_cluster_label='hadoop2',
        dbtap_id=data_store_id,
        db_table=source_table_name,
        hive_table='{}.{}'.format(database_name, source_table_name),
        hive_serde='orc'
    )


def find_data_store_id(data_store_name):
    data_stores = DbTap.list()
    found_data_stores = [data_store for data_store in data_stores if data_store.name == data_store_name]
    if len(found_data_stores) != 1:
        raise DataStoreNotFoundException()
    return found_data_stores[0].id


def run_hive_query_asynchronous(cluster_label, query_filename, **query_kwargs):
    template_loader = TemplateLoader(QUERY_DIR)
    query = template_loader.load_from_file(query_filename, **query_kwargs)
    return HiveCommand.create(query=query, label=cluster_label)


def list_cluster_names():
    clusters = Cluster.list()
    cluster_names = set(itertools.chain(*[cluster['cluster']['label'] for cluster in clusters]))
    return cluster_names
