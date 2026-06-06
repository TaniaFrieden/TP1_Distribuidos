import yaml
import json
import glob
import sys
import os

CONFIG_BASE = 'config/base.yml'
CONFIG_QUERIES = 'config/queries/*.json'
WORKER_TYPES_FILE = 'config/worker_types.json'
WORKERS_CONFIG_FILE = 'config/workers.json'

HEARTBEAT_INTERVAL_SECONDS = 5   # workers envían cada N segundos; watchdog usa el mismo valor para calcular timeout
WATCHDOG_MISSED_THRESHOLD = 6    # misses antes de declarar caída → timeout = HEARTBEAT_INTERVAL × MISSED_THRESHOLD
WATCHDOG_CHECK_INTERVAL_SECONDS = 5  # con qué frecuencia el hilo revisor del watchdog escanea


def _serializar_valor_env(valor):
    if isinstance(valor, (list, dict)):
        return json.dumps(valor)
    return str(valor)


def _expandir_input_queues(input_queue, worker_id):
    if isinstance(input_queue, list):
        colas = []
        for entrada in input_queue:
            if isinstance(entrada, str):
                if '{id}' in entrada:
                    colas.append(entrada.replace('{id}', worker_id))
                else:
                    colas.append(f"{entrada}_{worker_id}")
            else:
                colas.append(entrada)
        return colas

    if isinstance(input_queue, str):
        return input_queue.replace('{id}', worker_id)

    return input_queue


def _resolver_variables(obj, workers_config):
    if isinstance(obj, str) and obj.startswith('$'):
        key = obj[1:]
        if key not in workers_config:
            print(f"[WARN] Variable '{key}' no encontrada en workers.json, usando 1")
        return workers_config.get(key, 1)
    if isinstance(obj, dict):
        return {k: _resolver_variables(v, workers_config) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolver_variables(item, workers_config) for item in obj]
    return obj


def _generar_servicio(node, worker_config, workers_config, compose_data):
    node = _resolver_variables(node, workers_config)

    worker_type = node['type']
    base_config = worker_config.get(worker_type, {})
    prefix = node['prefix']
    replicas = node.get('replicas', 1)

    for i in range(1, replicas + 1):
        worker_id = str(i)
        worker_name = f"{prefix}_{i:02d}"

        env = base_config.get('default_env', {}).copy()
        env.update({
            'MOM_HOST': 'rabbitmq',
            'MOM_PORT': '5672',
            'MOM_USER': 'distributed',
            'MOM_PASSWORD': 'distributed',
            'MOM_VHOST': '/',
            'PYTHONMALLOC': 'malloc',
            'INPUT_QUEUES': _serializar_valor_env(
                _expandir_input_queues(node['input_queue'], worker_id)
            ),
            'OUTPUT_QUEUES': _serializar_valor_env(node['output_queue']),
            'NODE_PREFIX': prefix,
            'ID': worker_id,
            'TOTAL_WORKERS': str(replicas),
            'LOG_LEVEL': 'WARNING',
            'LOG_FILE': f'/app/logs/{worker_name}.txt'
        })
        env.update(node.get('extra_env', {}))

        compose_data['services'][worker_name] = {
            'build': {'context': './src', 'dockerfile': base_config['dockerfile']},
            'container_name': worker_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'},
                'gateway': {'condition': 'service_started'}
            },
            'volumes': ['./logs:/app/logs'],
            'environment': env
        }


def generar_compose():
    args = sys.argv[1:]

    with open(CONFIG_BASE, 'r') as f:
        compose_data = yaml.safe_load(f)

    with open(WORKER_TYPES_FILE, 'r') as f:
        worker_config = json.load(f)

    input_queues = []
    output_queues = []  # puede tener duplicados si varias queries comparten cola
    bank_queue_config = None
    watchdog_stages = []  # prefixes de todos los workers para monitorear

    query_files = sorted(glob.glob(CONFIG_QUERIES))

    for q_file in query_files:
        query_number = os.path.basename(q_file).replace('.json', '').replace('q', '')

        if args and query_number not in args:
            continue

        with open(q_file, 'r') as f:
            data = json.load(f)

        # Cola de entrada al gateway para esta query — override con "gateway_queue"
        gateway_out = data.get('gateway_queue', f"q{query_number}_raw_data")
        if gateway_out not in output_queues:
            output_queues.append(gateway_out)

        input_queues.append(f"q{query_number}_results")

        if bank_queue_config is None:
            bank_queue_config = data.get('bank_queue') or data.get('banks_shard')

        for node in data.get('workers', []):
            worker_type = node['type']
            base_config = worker_config.get(worker_type, {})
            prefix = node['prefix']
            replicas = node.get('replicas', 1)
def _resolver_variables(obj, workers_config):
    if isinstance(obj, str) and obj.startswith('$'):
        key = obj[1:]
        if key not in workers_config:
            print(f"[WARN] Variable '{key}' no encontrada en workers.json, usando 1")
        return workers_config.get(key, 1)
    if isinstance(obj, dict):
        return {k: _resolver_variables(v, workers_config) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolver_variables(item, workers_config) for item in obj]
    return obj


def _generar_servicio(node, worker_config, workers_config, compose_data):
    node = _resolver_variables(node, workers_config)

    worker_type = node['type']
    base_config = worker_config.get(worker_type, {})
    prefix = node['prefix']
    replicas = node.get('replicas', 1)

    for i in range(1, replicas + 1):
        worker_id = str(i)
        worker_name = f"{prefix}_{i:02d}"

        env = base_config.get('default_env', {}).copy()
        env.update({
            'MOM_HOST': 'rabbitmq',
            'MOM_PORT': '5672',
            'MOM_USER': 'distributed',
            'MOM_PASSWORD': 'distributed',
            'MOM_VHOST': '/',
            'PYTHONMALLOC': 'malloc',
            'INPUT_QUEUES': _serializar_valor_env(
                _expandir_input_queues(node['input_queue'], worker_id)
            ),
            'OUTPUT_QUEUES': _serializar_valor_env(node['output_queue']),
            'NODE_PREFIX': prefix,
            'ID': worker_id,
            'TOTAL_WORKERS': str(replicas),
            'HEARTBEAT_INTERVAL_SECONDS': str(HEARTBEAT_INTERVAL_SECONDS),
            'PREFETCH_COUNT': '10',
            'LOG_LEVEL': 'WARNING',
            'LOG_FILE': f'/app/logs/{worker_name}.txt'
        })
        env.update(node.get('extra_env', {}))

        compose_data['services'][worker_name] = {
            'build': {'context': './src', 'dockerfile': base_config['dockerfile']},
            'container_name': worker_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'},
                'gateway': {'condition': 'service_started'}
            },
            'volumes': ['./logs:/app/logs'],
            'environment': env
        }


def generar_compose():
    args = sys.argv[1:]

    with open(CONFIG_BASE, 'r') as f:
        compose_data = yaml.safe_load(f)

    with open(WORKER_TYPES_FILE, 'r') as f:
        worker_config = json.load(f)

    with open(WORKERS_CONFIG_FILE, 'r') as f:
        workers_config = json.load(f)

    input_queues = []
    output_queues = []
    bank_queue_config = None

    # Shared workers — siempre se generan
    for node in workers_config.get('shared_workers', []):
        _generar_servicio(node, worker_config, workers_config, compose_data)

    query_files = sorted(glob.glob(CONFIG_QUERIES))

    for q_file in query_files:
        query_number = os.path.basename(q_file).replace('.json', '').replace('q', '')

        if args and query_number not in args:
            continue

        with open(q_file, 'r') as f:
            data = json.load(f)

        gateway_out = data.get('gateway_queue', f"q{query_number}_raw_data")
        if gateway_out not in output_queues:
            output_queues.append(gateway_out)

        input_queues.append(f"q{query_number}_results")

        if bank_queue_config is None:
            raw = data.get('bank_queue') or data.get('banks_shard')
            if raw is not None:
                bank_queue_config = _resolver_variables(raw, workers_config)

        for node in data.get('workers', []):
            _generar_servicio(node, worker_config, workers_config, compose_data)

    if 'gateway' in compose_data['services']:
        env = compose_data['services']['gateway']['environment']
        env['OUTPUTS_QUEUE'] = ", ".join(output_queues)
        env['INPUTS_QUEUE'] = ", ".join(input_queues)
        env['LOG_FILE'] = '/app/logs/gateway.txt'
        env['MOM_PORT'] = '5672'
        env['MOM_USER'] = 'distributed'
        env['MOM_PASSWORD'] = 'distributed'
        env['MOM_VHOST'] = '/'
        if bank_queue_config is not None:
            env['BANK_QUEUE'] = _serializar_valor_env(bank_queue_config)
        compose_data['services']['gateway']['volumes'] = ['./logs:/app/logs']

    if 'rabbitmq' in compose_data['services']:
        compose_data['services']['rabbitmq'].setdefault('environment', {})
        rabbit_env = compose_data['services']['rabbitmq']['environment']
        rabbit_env['RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS'] = '-rabbit vm_memory_high_watermark 0.4 +MBas aobf +MBacul 10'
        compose_data['services']['rabbitmq'].setdefault('ports', [])
        if '15672:15672' not in compose_data['services']['rabbitmq']['ports']:
            compose_data['services']['rabbitmq']['ports'].append('15672:15672')

    # Recolectar dinámicamente los NODE_PREFIX de todos los servicios para monitorear
    watchdog_stages = []
    for s_name, s_data in compose_data.get('services', {}).items():
        if isinstance(s_data, dict) and 'environment' in s_data:
            prefix = s_data['environment'].get('NODE_PREFIX')
            if prefix and prefix not in watchdog_stages:
                watchdog_stages.append(prefix)

    # Watchdog — detecta caídas via heartbeats y publica en cola "caidas"
    compose_data['services']['watchdog'] = {
        'build': {'context': './src', 'dockerfile': 'watchdog/Dockerfile'},
        'container_name': 'watchdog',
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'},
            'gateway': {'condition': 'service_started'},
        },
        'volumes': ['./logs:/app/logs'],
        'environment': {
            'MOM_HOST': 'rabbitmq',
            'MOM_PORT': '5672',
            'MOM_USER': 'distributed',
            'MOM_PASSWORD': 'distributed',
            'MOM_VHOST': '/',
            'WATCHDOG_STAGES': json.dumps(watchdog_stages),
            'HEARTBEAT_INTERVAL_SECONDS': str(HEARTBEAT_INTERVAL_SECONDS),
            'MISSED_HEARTBEATS_THRESHOLD': str(WATCHDOG_MISSED_THRESHOLD),
            'CHECK_INTERVAL_SECONDS': str(WATCHDOG_CHECK_INTERVAL_SECONDS),
            'CAIDAS_QUEUE': 'caidas',
            'LOG_LEVEL': 'INFO',
            'LOG_FILE': '/app/logs/watchdog.txt',
        },
    }

    # Actuador — consume cola "caidas" y reinicia containers via Docker socket
    compose_data['services']['actuador'] = {
        'build': {'context': './src', 'dockerfile': 'watchdog/DockerfileActuador'},
        'container_name': 'actuador',
        'restart': 'always',
        'depends_on': {
            'rabbitmq': {'condition': 'service_healthy'},
        },
        'volumes': [
            '/var/run/docker.sock:/var/run/docker.sock',
            './logs:/app/logs',
        ],
        'environment': {
            'MOM_HOST': 'rabbitmq',
            'MOM_PORT': '5672',
            'MOM_USER': 'distributed',
            'MOM_PASSWORD': 'distributed',
            'MOM_VHOST': '/',
            'CAIDAS_QUEUE': 'caidas',
            'LOG_LEVEL': 'INFO',
            'LOG_FILE': '/app/logs/actuador.txt',
        },
    }

    with open('docker-compose.yml', 'w') as f:
        yaml.dump(compose_data, f, sort_keys=False, default_flow_style=False, width=1000)

    if args:
        nums = [a.lstrip('qQ') for a in args]
        formatted = ' '.join(f"q{n}" for n in nums)
        print(f"Docker-compose generado. Gateway configurado con: {formatted}")
    else:
        qnames = [q.split('_')[0] for q in output_queues if isinstance(q, str) and q.startswith('q')]
        print(f"Docker-compose generado. Gateway configurado con: {' '.join(qnames) or ', '.join(output_queues)}")


if __name__ == '__main__':
    generar_compose()