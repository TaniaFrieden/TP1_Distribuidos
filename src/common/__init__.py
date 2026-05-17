def __init__(self):

    self._cierre_solicitado = False
    self.mensajes_pendientes = 0
    self.condicion_pendiente = threading.Condition(threading.Lock())

    self._registrar_senales()

    mom_host         = os.getenv("MOM_HOST", "localhost")
    input_queue      = os.getenv("INPUT_QUEUE", "input_queue")
    control_exchange = os.getenv("CONTROL_EXCHANGE", "control_exchange")
    node_prefix      = os.getenv("NODE_PREFIX", "node")
    node_id          = int(os.getenv("ID", "0"))

    logging.info(f"[{self.__class__.__name__}] Conectando al middleware…")
    logging.info(f"{mom_host=}, {input_queue=}, {control_exchange=}, {node_prefix=}, {node_id=}")

    self.input_queue      = middleware.MessageMiddlewareQueueRabbitMQ(mom_host, input_queue)
    self.control_exchange = middleware.FanoutExchangeRabbitMQ(mom_host, control_exchange)
    self.control_queue    = middleware.FanoutQueueRabbitMQ(mom_host, f"{node_prefix}_{node_id}", control_exchange)