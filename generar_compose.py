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

NUM_WATCHDOGS = 3                        # instancias del watchdog (anillo de elección)
LEADER_HEARTBEAT_INTERVAL = 5            # con qué frecuencia el líder envía heartbeat a los standby
LEADER_TIMEOUT_SECONDS = 20              # tiempo sin heartbeat antes de que un standby inicie elección
ELECTION_STARTUP_DELAY_MAX = 3           # jitter máximo en segundos al arrancar antes de iniciar elección
CHECK_LEADER_INTERVAL = 5               # frecuencia del hilo que chequea timeout del líder
ELECTION_TIMEOUT = 30                    # segundos antes de reintentar una elección que no cerró
SUSPECTED_DEAD_TTL = 60                  # segundos que un nodo permanece en la lista de sospechados

PREFETCH_WORKER_QUEUE = 1
PREFETCH_SHARD_QUEUE = 1000

NUM_ACTUADORES = 2                       # instancias del actuador (consume cola "caidas" en paralelo)


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


def _image_name_from_dockerfile(dockerfile):
    parts = dockerfile.split('/')
    filename = parts[-1]
    if filename == 'Dockerfile':
        dir_name = parts[-2] if len(parts) > 1 else 'base'
    else:
        dir_name = filename.replace('Dockerfile', '').lower()
    return f"tp1-{dir_name}"


def _generar_servicio(node, worker_config, workers_config, compose_data, built_images):
    node = _resolver_variables(node, workers_config)

    worker_type = node['type']
    base_config = worker_config.get(worker_type, {})
    prefix = node['prefix']
    replicas = node.get('replicas', 1)
    dockerfile = base_config['dockerfile']
    image_name = _image_name_from_dockerfile(dockerfile)

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
            'LOG_LEVEL': 'INFO',
            'LOG_FILE': f'/app/logs/{worker_name}.txt'
        })
        env.update(node.get('extra_env', {}))
        input_q = node['input_queue']
        cola_compartida = (
            isinstance(input_q, str)
            and '{id}' not in input_q
            and replicas > 1
        )
        prefetch = PREFETCH_WORKER_QUEUE if cola_compartida else PREFETCH_SHARD_QUEUE
        env.setdefault('PREFETCH_COUNT', prefetch)

        instance_var = worker_name.upper()
        env['CRASH_HOOK'] = f'${{{instance_var}_CRASH:-}}'

        volumes = ['./logs:/app/logs', f'./volume/{worker_name}:/app/volumen']

        service = {
            'container_name': worker_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'},
                'gateway': {'condition': 'service_started'}
            },
            'volumes': volumes,
            'environment': env
        }

        if dockerfile not in built_images:
            built_images[dockerfile] = image_name
            service['build'] = {'context': './src', 'dockerfile': dockerfile}
            service['image'] = image_name
        else:
            service['image'] = image_name

        compose_data['services'][worker_name] = service


def generar_compose():
    args = sys.argv[1:]

    with open(CONFIG_BASE, 'r') as f:
        compose_data = yaml.safe_load(f)

    with open(WORKER_TYPES_FILE, 'r') as f:
        worker_config = json.load(f)

    with open(WORKERS_CONFIG_FILE, 'r') as f:
        workers_config = json.load(f)

    built_images = {}

    input_queues = []
    output_queues = []
    bank_queue_config = None

    # Shared workers — siempre se generan, pero sus output queues se filtran
    # para incluir solo las que corresponden a las queries seleccionadas.
    selected_queries = set(args) if args else None
    for node in workers_config.get('shared_workers', []):
        if selected_queries and isinstance(node.get('output_queue'), list):
            node = dict(node)
            node['output_queue'] = [
                q for q in node['output_queue']
                if any(q.startswith(f"q{qn}_") for qn in selected_queries)
            ]
        _generar_servicio(node, worker_config, workers_config, compose_data, built_images)

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
            _generar_servicio(node, worker_config, workers_config, compose_data, built_images)

    if 'gateway' in compose_data['services']:
        gw = compose_data['services']['gateway']
        gw['image'] = 'tp1-gateway'
        env = gw['environment']
        env['OUTPUTS_QUEUE'] = ", ".join(output_queues)
        env['INPUTS_QUEUE'] = ", ".join(input_queues)
        env['LOG_FILE'] = '/app/logs/gateway.txt'
        env['MOM_PORT'] = '5672'
        env['MOM_USER'] = 'distributed'
        env['MOM_PASSWORD'] = 'distributed'
        env['MOM_VHOST'] = '/'
        if bank_queue_config is not None:
            env['BANK_QUEUE'] = _serializar_valor_env(bank_queue_config)
        env['CRASH_HOOK'] = '${GATEWAY_01_CRASH:-}'
        gw['volumes'] = [
            './logs:/app/logs',
            './volume/gateway:/app/volumen',
        ]

    if 'client' in compose_data['services']:
        compose_data['services']['client']['image'] = 'tp1-client'

    if 'rabbitmq' in compose_data['services']:
        compose_data['services']['rabbitmq'].setdefault('environment', {})
        rabbit_env = compose_data['services']['rabbitmq']['environment']
        rabbit_env['RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS'] = '-rabbit vm_memory_high_watermark 0.4 +MBas aobf +MBacul 10'
        compose_data['services']['rabbitmq'].setdefault('ports', [])
        if '15672:15672' not in compose_data['services']['rabbitmq']['ports']:
            compose_data['services']['rabbitmq']['ports'].append('15672:15672')

    # Recolectar dinámicamente los NODE_PREFIX de todos los servicios para monitorear.
    # El gateway no tiene NODE_PREFIX pero también debe monitorearse.
    # 'actuador' se agrega explícitamente porque se genera después de este loop.
    watchdog_stages = ['gateway']
    for s_name, s_data in compose_data.get('services', {}).items():
        if isinstance(s_data, dict) and 'environment' in s_data:
            prefix = s_data['environment'].get('NODE_PREFIX')
            if prefix and prefix not in watchdog_stages:
                watchdog_stages.append(prefix)
    watchdog_stages.append('actuador')

    watchdog_dockerfile = 'watchdog/Dockerfile'
    watchdog_image = _image_name_from_dockerfile(watchdog_dockerfile)

    # Watchdog — 3 instancias con elección en anillo; sólo el líder activa el detector de caídas
    for wid in range(1, NUM_WATCHDOGS + 1):
        service_name = f"watchdog_{wid}"
        service = {
            'container_name': service_name,
            'restart': 'on-failure',
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'},
                'gateway': {'condition': 'service_started'},
            },
            'volumes': ['./logs:/app/logs', f'./volume/{service_name}:/app/volumen'],
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
                'WATCHDOG_ID': str(wid),
                'NUM_WATCHDOGS': str(NUM_WATCHDOGS),
                'LEADER_HEARTBEAT_INTERVAL': str(LEADER_HEARTBEAT_INTERVAL),
                'LEADER_TIMEOUT_SECONDS': str(LEADER_TIMEOUT_SECONDS),
                'ELECTION_STARTUP_DELAY_MAX': str(ELECTION_STARTUP_DELAY_MAX),
                'CHECK_LEADER_INTERVAL': str(CHECK_LEADER_INTERVAL),
                'ELECTION_TIMEOUT': str(ELECTION_TIMEOUT),
                'SUSPECTED_DEAD_TTL': str(SUSPECTED_DEAD_TTL),
                'LOG_LEVEL': 'INFO',
                'LOG_FILE': f'/app/logs/watchdog_{wid}.txt',
                'CRASH_HOOK': f'${{WATCHDOG_{wid}_CRASH:-}}',
            },
        }
        if watchdog_dockerfile not in built_images:
            built_images[watchdog_dockerfile] = watchdog_image
            service['build'] = {'context': './src', 'dockerfile': watchdog_dockerfile}
            service['image'] = watchdog_image
        else:
            service['image'] = watchdog_image
        compose_data['services'][service_name] = service

    actuador_dockerfile = 'watchdog/DockerfileActuador'
    actuador_image = _image_name_from_dockerfile(actuador_dockerfile)

    # Actuador — múltiples instancias consumen la cola "caidas" en paralelo
    # Monitoreados por el watchdog via heartbeat; otro actuador lo reinicia si cae.
    for aid in range(1, NUM_ACTUADORES + 1):
        service_name = f"actuador_{aid:02d}"
        service = {
            'container_name': service_name,
            'depends_on': {
                'rabbitmq': {'condition': 'service_healthy'},
            },
            'volumes': [
                '/var/run/docker.sock:/var/run/docker.sock',
                './logs:/app/logs',
                f'./volume/{service_name}:/app/volumen',
            ],
            'environment': {
                'MOM_HOST': 'rabbitmq',
                'MOM_PORT': '5672',
                'MOM_USER': 'distributed',
                'MOM_PASSWORD': 'distributed',
                'MOM_VHOST': '/',
                'CAIDAS_QUEUE': 'caidas',
                'NODE_PREFIX': 'actuador',
                'ID': str(aid),
                'TOTAL_WORKERS': str(NUM_ACTUADORES),
                'HEARTBEAT_INTERVAL_SECONDS': str(HEARTBEAT_INTERVAL_SECONDS),
                'LOG_LEVEL': 'INFO',
                'LOG_FILE': f'/app/logs/{service_name}.txt',
            },
        }
        if actuador_dockerfile not in built_images:
            built_images[actuador_dockerfile] = actuador_image
            service['build'] = {'context': './src', 'dockerfile': actuador_dockerfile}
            service['image'] = actuador_image
        else:
            service['image'] = actuador_image
        compose_data['services'][service_name] = service

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