import argparse
import os

import functools

import logging

import sys
import qds_sdk

from flask import (
    Flask,
    render_template,
    request,
    session,
    jsonify,
    abort
)
from flask import Response
from flask import redirect
from flask import url_for
from qds_sdk.cluster import Cluster
from qds_sdk.qubole import Qubole
from qds_sdk.exception import ResourceNotFound

from qubole_root import PROJECT_DIR
from utils.config import read_config
from utils.qubole import (
    run_hive_query_asynchronous,
    find_data_store_id,
    import_data_table,
    create_hadoop_cluster,
    create_spark_cluster,
    import_spark_notebook,
    import_dashboard_notebook,
    list_cluster_names
)

app = Flask(
    __name__,
    template_folder=os.path.join(PROJECT_DIR, 'web/templates'),
    static_folder=os.path.join(PROJECT_DIR, 'web/static')
)


def login_required(fun):

    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        if session.get('logged_in') is None:
            abort(401)
        return fun(*args, **kwargs)

    return wrapper


def make_session_state():
    return jsonify({
        'current_step': session['current_step']
    })


def mark_step_as_done(step):

    def outer(fun):

        @functools.wraps(fun)
        def inner(*args, **kwargs):
            fun(*args, **kwargs)
            return make_session_state()

        return inner

    return outer


@app.route('/login', methods=['POST'])
def login():
    if request.form['username'] == app.config['webapp_username'] and \
                    request.form['password'] == app.config['webapp_password']:
        session['logged_in'] = True
        session['current_step'] = 1
        return redirect(url_for('wizard'))
    else:
        return render_template('login.html')


@app.route('/step', methods=['GET', 'POST'])
@login_required
def step():
    if request.method == 'POST':
        data = request.get_json(force=True)
        if 'step' in data:
            session['current_step'] = data['step']
    return make_session_state()


@app.route('/')
def home():
    if session.get('logged_in') is None:
        return render_template('login.html')
    return redirect(url_for('wizard'))


@app.route('/wizard')
def wizard():
    if session.get('logged_in') is None:
        return render_template('login.html')
    return render_template('wizard.html')


@app.route('/create_clusters_and_notebooks', methods=['POST'])
@login_required
def create_clusters_and_notebooks():
    logger = app.logger

    try:
        Cluster.delete(config['hadoop_cluster_name'])
    except ResourceNotFound as e:
        logger.error("Error when removing Hadoop cluster configuration. {}".format(e))
        # Cluster might be deleted so it should be possible to create it again.
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when removing Hadoop cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    try:
        hadoop_cluster_response = create_hadoop_cluster(config)
        logger.info("Create Hadoop cluster response {}".format(hadoop_cluster_response))
        hadoop_cluster_id = hadoop_cluster_response['id']
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when creating Hadoop cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    try:
        Cluster.start(config['hadoop_cluster_name'])
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when launching Hadoop cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    try:
        Cluster.delete(config['spark_cluster_name'])
    except ResourceNotFound as e:
        logger.error("Error when removing Spark cluster configuration. {}".format(e))
        # Cluster might be deleted so it should be possible to create it again.
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when removing Spark cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    try:
        spark_cluster_response = create_spark_cluster(config)
        logger.info("Create Spark cluster response {}".format(spark_cluster_response))
        spark_cluster_id = spark_cluster_response['id']
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when creating Spark cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    try:
        Cluster.start(config['spark_cluster_name'])
    except qds_sdk.exception.Error as e:
        json_response = e.request.json()
        logger.error("Error when launching Hadoop cluster configuration. {}".format(json_response))
        return jsonify(json_response)

    spark_notebook = import_spark_notebook(config, spark_cluster_id=spark_cluster_id)
    if not spark_notebook['success']:
        logger.error('Failed to import Spark Notebook. {}'.format(spark_notebook))
        resp = {
            'error': {
                'error_message': "Notebook {} {}".format(config['spark_notebook_name'], spark_notebook['message'])
            }
        }
        return jsonify(resp)

    dashboard_notebook = import_dashboard_notebook(config, spark_cluster_id=spark_cluster_id)
    if not dashboard_notebook['success']:
        logger.error('Failed to import Dashboard Notebook. {}'.format(dashboard_notebook))
        resp = {
            'error': {
                'error_message': "Notebook {} {}".format(config['spark_dashboard_notebook_name'], dashboard_notebook['message'])
            }
        }
        return jsonify(resp)

    new_config = {
        'spark_cluster_id': spark_cluster_id,
        'hadoop_cluster_id': hadoop_cluster_id,
        'spark_notebook_id': spark_notebook['id'],
        'dashboard_notebook_id': dashboard_notebook['id']
    }
    logger.info('Updating app.config to {}'.format(new_config))
    app.config.update(new_config)
    return Response()


@app.route('/import_tables', methods=['POST'])
@login_required
def import_tables():
    data_store_id = find_data_store_id(config['qubole_data_store_name'])
    tables_to_import = ['customers', 'departments', 'order_items', 'orders', 'products']
    for table_name in tables_to_import:
        import_data_table(data_store_id, table_name, config['qubole_database_name'])
    return Response()


@app.route('/run_query', methods=['POST'])
@login_required
def run_query():
    query_mapping = {
        'web_logs': 'create_web_logs_table.sql',
        'top_categories': 'top_10_most_popular_categories.sql',
        'top_products': 'top_10_most_viewed_products.sql',
        'top_revenue': 'top_10_revenue_generating_products.sql'
    }
    data = request.get_json(force=True)
    query_filename = query_mapping[data['query_name']]
    run_hive_query_asynchronous(
        cluster_label=config['hadoop_cluster_name'],
        query_filename=query_filename,
        qubole_web_logs_s3_dir=config['qubole_web_logs_s3_dir'],
        qubole_database_name=config['qubole_database_name']
    )
    return Response()


@app.route('/run_scaling', methods=['POST'])
@login_required
def run_scaling():
    for _ in range(10):
        run_hive_query_asynchronous(
            cluster_label=config['hadoop_cluster_name'],
            query_filename='top_10_revenue_generating_products.sql',
            qubole_database_name=config['qubole_database_name']
        )
    return Response()


def parse_command_line_args():
    parser = argparse.ArgumentParser(description='Quick start App')
    parser.add_argument('--config', required=True, help='Configuration')
    parser.add_argument('--extra-config', help='Configuration of clusters and notebooks')
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    args = parse_command_line_args()
    config = read_config(args.config)
    app.secret_key = os.urandom(47)
    app.config.update(config)
    Qubole.configure(api_token=config['qubole_api_token'])
    app.run(host='0.0.0.0', port=int(config['port']), threaded=True)
