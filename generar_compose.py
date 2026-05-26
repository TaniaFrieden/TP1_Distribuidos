import yaml
import json
import glob
import sys
import os

CONFIG_BASE = 'config/base.yml'
CONFIG_QUERIES = 'config/queries/*.json'
WORKER_TYPES_FILE = 'config/worker_types.json' 


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

def generar_compose():
    args = sys.argv[1:]
    
    # 1. Cargar infraestructura base
    with open(CONFIG_BASE, 'r') as f:
        compose_data = yaml.safe_load(f)
    
    # 2. Cargar tipos de workers
    with open(WORKER_TYPES_FILE, 'r') as f:
        worker_config = json.load(f)

    input_queues = []
    output_queues = []
    bank_queue_config = None

    # 3. Leer archivos JSON de queries
    query_files = glob.glob(CONFIG_QUERIES)
    
    for q_file in query_files:
        query_number = os.path.basename(q_file).replace('.json', '').replace('q', '')
        
        if args and query_number not in args:
            continue
            
        with open(q_file, 'r') as f:
            data = json.load(f)
        
        # Agregamos al gateway
        output_queues.append(f"q{query_number}_raw_data")
        input_queues.append(f"q{query_number}_results")

        if bank_queue_config is None:
            bank_queue_config = data.get('bank_queue') or data.get('banks_shard')
        
        # --- PROCESAMIENTO DE WORKERS ---
        for node in data.get('workers', []):
            worker_type = node['type']
            base_config = worker_config.get(worker_type, {})
            prefix = node['prefix']
            replicas = node.get('replicas', 1)
            
            for i in range(1, replicas + 1):
                worker_id = str(i)
                worker_name = f"{prefix}_{i:02d}"
                
                # Construir entorno
                env = base_config.get('default_env', {}).copy()
                env.update({
                    'MOM_HOST': 'rabbitmq',
                    'MOM_PORT': '5672',
                    'MOM_USER': 'distributed',
                    'MOM_PASSWORD': 'distributed',
                    'MOM_VHOST': '/',
                    'INPUT_QUEUES': _serializar_valor_env(
                        _expandir_input_queues(node['input_queue'], worker_id)
                    ),
                    'OUTPUT_QUEUES': _serializar_valor_env(node['output_queue']),
                    'NODE_PREFIX': prefix,
                    'ID': worker_id,
                    'TOTAL_WORKERS': str(replicas),
                    'LOG_FILE': f'/app/logs/{worker_name}.txt'
                })
                env.update(node.get('extra_env', {}))
                
                # Inyectar en compose_data
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

    # 4. Actualizar el Gateway
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
        rabbit_env['RABBITMQ_DEFAULT_USER'] = 'distributed'
        rabbit_env['RABBITMQ_DEFAULT_PASS'] = 'distributed'
        rabbit_env['RABBITMQ_DEFAULT_VHOST'] = '/'

    # 5. Guardar
    with open('docker-compose.yml', 'w') as f:
        yaml.dump(compose_data, f, sort_keys=False, default_flow_style=False, width=1000)

    args = sys.argv[1:]
    if args:
        nums = [a.lstrip('qQ') for a in args]
        formatted = ' '.join(f"q{n}" for n in nums)
        print(f"Docker-compose generado. Gateway configurado con: {formatted}")
    else:
        qnames = []
        for q in output_queues:
            if isinstance(q, str) and q.startswith('q'):
                qnames.append(q.split('_')[0])
        if qnames:
            print(f"Docker-compose generado. Gateway configurado con: {' '.join(qnames)}")
        else:
            print(f"Docker-compose generado. Gateway configurado con: {', '.join(output_queues)}")
if __name__ == '__main__':
    generar_compose()