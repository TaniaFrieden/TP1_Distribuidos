import json
import logging
import random
import threading
import time

from common.middleware.middleware_rabbitmq import (
    FanoutExchangeRabbitMQ,
    FanoutQueueRabbitMQ,
    MessageMiddlewareQueueRabbitMQ,
)

logger = logging.getLogger(__name__)

LEADER_HB_EXCHANGE = "heartbeat.watchdog"


class RingElection:
    """
    Protocolo de elección en anillo entre instancias del watchdog.

    Topología del anillo: watchdog_1 → watchdog_2 → watchdog_3 → watchdog_1
    Colas de anillo:     ring.1, ring.2, ring.3

    Mensajes:
      {"tipo": "eleccion",    "id": N}  — propagación de elección
      {"tipo": "coordinador", "id": N}  — anuncio del líder electo

    El líder publica heartbeats en el exchange fanout LEADER_HB_EXCHANGE.
    Cada nodo tiene su propia cola heartbeat.watchdog.<id> ligada a ese exchange
    para recibir todos los heartbeats sin round-robin.
    """

    def __init__(self, config, on_become_leader, on_lose_leader):
        self._config = config
        self._on_become_leader = on_become_leader
        self._on_lose_leader = on_lose_leader

        self._id = config.watchdog_id
        self._n = config.num_watchdogs
        self._next_id = (self._id % self._n) + 1  # anillo 1-indexed

        self._is_leader = False
        self._leader_id = None
        self._in_election = False
        self._last_leader_hb = None          # None = aún no se recibió heartbeat
        self._forwarded_coordinators = set() # evitar reenviar el mismo coordinador

        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        self._ring_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._ring_sender: MessageMiddlewareQueueRabbitMQ | None = None
        self._hb_consumer: FanoutQueueRabbitMQ | None = None
        self._hb_publisher: FanoutExchangeRabbitMQ | None = None

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def start(self):
        threading.Thread(
            target=self._consume_ring, daemon=True, name=f"ring-{self._id}"
        ).start()
        threading.Thread(
            target=self._consume_leader_hb, daemon=True, name=f"ring-hb-{self._id}"
        ).start()
        threading.Thread(
            target=self._check_leader_timeout, daemon=True, name=f"ring-check-{self._id}"
        ).start()
        threading.Thread(
            target=self._startup_check, daemon=True, name=f"ring-init-{self._id}"
        ).start()
        logger.info(
            f"[Ring-{self._id}] Iniciado. Siguiente en anillo: watchdog_{self._next_id}. "
            f"Total nodos: {self._n}"
        )

    def stop(self):
        self._stop_event.set()
        for q in [self._ring_consumer, self._hb_consumer]:
            if q:
                try:
                    q.stop_consuming()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Chequeo inicial: iniciar elección si no hay líder
    # ------------------------------------------------------------------

    def _startup_check(self):
        delay = random.uniform(0, self._config.election_startup_delay_max)
        logger.info(f"[Ring-{self._id}] Esperando {delay:.1f}s antes de verificar líder.")
        if self._stop_event.wait(delay):
            return

        with self._lock:
            hb_received = self._last_leader_hb is not None

        if hb_received:
            logger.info(f"[Ring-{self._id}] Líder activo detectado vía heartbeat. No se inicia elección.")
        else:
            logger.info(f"[Ring-{self._id}] Sin heartbeat de líder. Iniciando elección.")
            self._initiate_election()

    # ------------------------------------------------------------------
    # Elección
    # ------------------------------------------------------------------

    def _initiate_election(self):
        with self._lock:
            if self._in_election:
                logger.debug(f"[Ring-{self._id}] Elección ya en progreso, omitiendo inicio duplicado.")
                return
            self._in_election = True

        logger.info(f"[Ring-{self._id}] Elección iniciada — enviando id={self._id} a watchdog_{self._next_id}.")
        self._send_to_next({"tipo": "eleccion", "id": self._id})

    def _handle_election(self, received_id: int):
        with self._lock:
            is_leader = self._is_leader
            leader_id = self._leader_id
            last_hb = self._last_leader_hb

        max_id = max(received_id, self._id)

        if received_id == self._id:
            # El propio mensaje dio la vuelta completa al anillo
            if is_leader:
                logger.debug(f"[Ring-{self._id}] Ya soy líder, ignorando elección duplicada.")
                return
            # Verificar que no haya un líder más reciente activo
            if (leader_id is not None
                    and leader_id != self._id
                    and last_hb is not None
                    and time.time() - last_hb < self._config.leader_timeout_seconds):
                logger.info(
                    f"[Ring-{self._id}] Ignorando elección propia: "
                    f"watchdog_{leader_id} ya es líder activo."
                )
                return
            self._declare_leader()
        else:
            # Swallow si ya hay un líder activo conocido
            if is_leader:
                logger.debug(f"[Ring-{self._id}] Soy líder, absorbiendo mensaje de elección.")
                return
            if (leader_id is not None
                    and last_hb is not None
                    and time.time() - last_hb < self._config.leader_timeout_seconds):
                logger.debug(
                    f"[Ring-{self._id}] Líder activo conocido, absorbiendo mensaje de elección."
                )
                return
            with self._lock:
                self._in_election = True
            logger.debug(f"[Ring-{self._id}] Reenviando elección id={max_id} (recibido={received_id}).")
            self._send_to_next({"tipo": "eleccion", "id": max_id})

    def _declare_leader(self):
        logger.info(f"[Ring-{self._id}] ¡SOY EL LÍDER!")
        with self._lock:
            self._is_leader = True
            self._leader_id = self._id
            self._in_election = False

        self._on_become_leader()
        self._send_to_next({"tipo": "coordinador", "id": self._id})
        threading.Thread(
            target=self._leader_hb_loop, daemon=True, name=f"ring-hb-send-{self._id}"
        ).start()

    def _handle_coordinator(self, leader_id: int):
        if leader_id == self._id:
            # El coordinador completó la vuelta o es un stale message tras reinicio
            with self._lock:
                if not self._is_leader:
                    logger.debug(
                        f"[Ring-{self._id}] Coordinador con id propio pero no soy líder — "
                        "mensaje obsoleto, ignorando."
                    )
            return

        with self._lock:
            already_forwarded = leader_id in self._forwarded_coordinators
            if not already_forwarded:
                self._forwarded_coordinators.add(leader_id)
            prev_leader = self._leader_id
            self._leader_id = leader_id
            self._last_leader_hb = time.time()
            self._in_election = False
            was_leader = self._is_leader
            self._is_leader = False

        logger.info(f"[Ring-{self._id}] Líder establecido: watchdog_{leader_id}")

        if was_leader:
            self._on_lose_leader()

        if not already_forwarded:
            self._send_to_next({"tipo": "coordinador", "id": leader_id})

    # ------------------------------------------------------------------
    # Hilo: consumidor de mensajes del anillo
    # ------------------------------------------------------------------

    def _consume_ring(self):
        queue_name = f"ring.{self._id}"
        try:
            q = MessageMiddlewareQueueRabbitMQ(self._config.mom_host, queue_name)
            self._ring_consumer = q
            logger.info(f"[Ring-{self._id}] Escuchando cola {queue_name}")
            q.start_consuming(self._on_ring_message)
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"[Ring-{self._id}] Error en cola {queue_name}: {e}", exc_info=True)

    def _on_ring_message(self, msg: bytes, ack, nack):
        try:
            payload = json.loads(msg.decode("utf-8"))
            tipo = payload.get("tipo")
            received_id = payload.get("id")
            ack()
            if tipo == "eleccion":
                self._handle_election(received_id)
            elif tipo == "coordinador":
                self._handle_coordinator(received_id)
            else:
                logger.warning(f"[Ring-{self._id}] Tipo de mensaje desconocido: {tipo}")
        except Exception as e:
            logger.warning(f"[Ring-{self._id}] Mensaje de anillo malformado: {e}")
            ack()

    # ------------------------------------------------------------------
    # Hilo: heartbeat sender del líder
    # ------------------------------------------------------------------

    def _leader_hb_loop(self):
        try:
            pub = FanoutExchangeRabbitMQ(self._config.mom_host, LEADER_HB_EXCHANGE)
            self._hb_publisher = pub
            while not self._stop_event.is_set():
                with self._lock:
                    if not self._is_leader:
                        break
                hb = json.dumps(
                    {"tipo": "lider_hb", "id": self._id, "timestamp": time.time()}
                ).encode()
                try:
                    pub.send(hb)
                    logger.debug(f"[Ring-{self._id}] Heartbeat de líder enviado.")
                except Exception as e:
                    logger.warning(f"[Ring-{self._id}] Error enviando heartbeat de líder: {e}")
                self._stop_event.wait(self._config.leader_heartbeat_interval)
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(f"[Ring-{self._id}] Error en loop heartbeat líder: {e}", exc_info=True)

    # ------------------------------------------------------------------
    # Hilo: consumidor de heartbeats del líder (para standby)
    # ------------------------------------------------------------------

    def _consume_leader_hb(self):
        queue_name = f"heartbeat.watchdog.{self._id}"
        try:
            q = FanoutQueueRabbitMQ(self._config.mom_host, queue_name, LEADER_HB_EXCHANGE)
            self._hb_consumer = q
            logger.info(
                f"[Ring-{self._id}] Monitoreando heartbeats de líder en {queue_name} "
                f"(exchange: {LEADER_HB_EXCHANGE})"
            )
            q.start_consuming(self._on_leader_hb)
        except Exception as e:
            if not self._stop_event.is_set():
                logger.error(
                    f"[Ring-{self._id}] Error monitoreando heartbeat líder: {e}", exc_info=True
                )

    def _on_leader_hb(self, msg: bytes, ack, nack):
        try:
            payload = json.loads(msg.decode("utf-8"))
            leader_id = payload.get("id")
            with self._lock:
                self._last_leader_hb = time.time()
                if not self._is_leader and leader_id is not None:
                    self._leader_id = leader_id
            logger.debug(f"[Ring-{self._id}] Heartbeat recibido de líder watchdog_{leader_id}.")
            ack()
        except Exception as e:
            logger.warning(f"[Ring-{self._id}] Heartbeat de líder malformado: {e}")
            ack()

    # ------------------------------------------------------------------
    # Hilo: detector de timeout del líder
    # ------------------------------------------------------------------

    def _check_leader_timeout(self):
        startup_time = time.time()
        while not self._stop_event.wait(self._config.check_leader_interval):
            with self._lock:
                if self._is_leader:
                    continue
                last_hb = self._last_leader_hb

            if last_hb is None:
                elapsed = time.time() - startup_time
            else:
                elapsed = time.time() - last_hb

            if elapsed > self._config.leader_timeout_seconds:
                logger.warning(
                    f"[Ring-{self._id}] Sin heartbeat de líder por {elapsed:.1f}s. "
                    "Iniciando nueva elección."
                )
                with self._lock:
                    self._leader_id = None
                    self._last_leader_hb = None
                    self._forwarded_coordinators.clear()
                self._initiate_election()

    # ------------------------------------------------------------------
    # Helper: enviar al siguiente nodo del anillo
    # ------------------------------------------------------------------

    def _send_to_next(self, payload: dict):
        queue_name = f"ring.{self._next_id}"
        data = json.dumps(payload).encode()
        try:
            if self._ring_sender is None:
                self._ring_sender = MessageMiddlewareQueueRabbitMQ(
                    self._config.mom_host, queue_name
                )
            self._ring_sender.send(data)
        except Exception as e:
            logger.error(
                f"[Ring-{self._id}] Error enviando {payload['tipo']} a {queue_name}: {e}",
                exc_info=True,
            )
            self._ring_sender = None
            try:
                self._ring_sender = MessageMiddlewareQueueRabbitMQ(
                    self._config.mom_host, queue_name
                )
                self._ring_sender.send(data)
            except Exception as e2:
                logger.error(
                    f"[Ring-{self._id}] Retry fallido enviando a {queue_name}: {e2}",
                    exc_info=True,
                )
