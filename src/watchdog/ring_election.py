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

    Topología: watchdog_1 → watchdog_2 → watchdog_3 → watchdog_1  (ring.N queues)

    Mensajes por el anillo:
      eleccion    — propaga max(received_id, self_id) hacia el siguiente nodo
      coordinador — anuncia el líder electo, da una vuelta completa
      vivo        — nodo reiniciado avisa que está activo
      hb_standby  — standby avisa periódicamente al líder que sigue vivo

    Heartbeats fuera del anillo:
      líder → standbys : fanout exchange LEADER_HB_EXCHANGE
    """

    def __init__(self, config, on_become_leader, on_lose_leader, on_standby_dead=None):
        self._config = config
        self._on_become_leader = on_become_leader
        self._on_lose_leader = on_lose_leader
        self._on_standby_dead = on_standby_dead  # callback(node_id: int)

        self._id = config.watchdog_id
        self._n = config.num_watchdogs
        self._next_id = (self._id % self._n) + 1  # anillo 1-indexed

        self._is_leader = False
        self._leader_id = None
        self._in_election = False
        self._election_started_at: float | None = None
        self._last_leader_hb: float | None = None
        self._last_election_target: int | None = None
        self._forwarded_coordinators: set[int] = set()
        self._suspected_dead_ids: dict[int, float] = {}  # nodo_id → timestamp sospechado

        # Estado exclusivo del líder para monitorear standbys
        self._became_leader_at: float | None = None
        self._standby_last_seen: dict[int, float] = {}   # nodo_id → último hb recibido
        self._reported_dead_standbys: set[int] = set()   # ya publicados a caidas

        self._lock = threading.Lock()
        self._send_lock = threading.Lock()  # pika no es thread-safe; serializa envíos
        self._stop_event = threading.Event()

        self._ring_consumer: MessageMiddlewareQueueRabbitMQ | None = None
        self._ring_senders: dict[int, MessageMiddlewareQueueRabbitMQ] = {}
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
            target=self._periodic_loop, daemon=True, name=f"ring-periodic-{self._id}"
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
    # Chequeo inicial
    # ------------------------------------------------------------------

    def _startup_check(self):
        delay = random.uniform(0, self._config.election_startup_delay_max)
        logger.info(f"[Ring-{self._id}] Esperando {delay:.1f}s antes de verificar líder.")
        if self._stop_event.wait(delay):
            return

        with self._lock:
            hb_received = self._last_leader_hb is not None

        if hb_received:
            logger.info(f"[Ring-{self._id}] Líder activo detectado. Anunciando presencia al anillo.")
            self._announce_alive()
        else:
            logger.info(f"[Ring-{self._id}] Sin heartbeat de líder. Iniciando elección.")
            self._initiate_election()

    # ------------------------------------------------------------------
    # Anuncio de presencia al reiniciarse como standby
    # ------------------------------------------------------------------

    def _announce_alive(self):
        msg = {"tipo": "vivo", "id": self._id}
        for nid in range(1, self._n + 1):
            if nid != self._id:
                self._send_to(nid, msg)
        logger.info(f"[Ring-{self._id}] Anuncio 'vivo' enviado a todos los nodos.")

    def _handle_alive(self, node_id: int):
        with self._lock:
            self._suspected_dead_ids.pop(node_id, None)
            if self._is_leader:
                self._standby_last_seen[node_id] = time.time()
                self._reported_dead_standbys.discard(node_id)
        logger.info(f"[Ring-{self._id}] watchdog_{node_id} anunció que está vivo → removido de sospechados.")

    # ------------------------------------------------------------------
    # Elección
    # ------------------------------------------------------------------

    def _initiate_election(self):
        with self._lock:
            if self._in_election:
                elapsed = (
                    time.time() - self._election_started_at
                    if self._election_started_at is not None
                    else 0
                )
                if elapsed < self._config.election_timeout:
                    return
                if (self._last_election_target is not None
                        and self._last_election_target != self._id):
                    self._suspected_dead_ids.setdefault(
                        self._last_election_target, time.time()
                    )
                    logger.warning(
                        f"[Ring-{self._id}] Timeout de elección: "
                        f"watchdog_{self._last_election_target} agregado a sospechados."
                    )
                logger.warning(
                    f"[Ring-{self._id}] Elección sin resultado tras {elapsed:.0f}s. "
                    "Reintentando saltando nodos sospechados."
                )
            self._in_election = True
            self._election_started_at = time.time()
            skip = list(self._suspected_dead_ids)

        target = self._get_next_target()
        with self._lock:
            self._last_election_target = target
        logger.info(
            f"[Ring-{self._id}] Elección iniciada — "
            f"enviando id={self._id} a watchdog_{target} (skip={skip})."
        )
        self._send_to(target, {"tipo": "eleccion", "id": self._id, "skip": skip})

    def _handle_election(self, received_id: int, skip: list[int]):
        with self._lock:
            if skip:
                now = time.time()
                for nid in skip:
                    self._suspected_dead_ids.setdefault(nid, now)
            is_leader = self._is_leader
            leader_id = self._leader_id
            last_hb = self._last_leader_hb

        max_id = max(received_id, self._id)

        if received_id == self._id:
            if is_leader:
                logger.debug(f"[Ring-{self._id}] Ya soy líder, ignorando elección duplicada.")
                return
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
            if is_leader:
                logger.debug(f"[Ring-{self._id}] Soy líder, absorbiendo mensaje de elección.")
                return
            if (leader_id is not None
                    and last_hb is not None
                    and time.time() - last_hb < self._config.leader_timeout_seconds):
                logger.debug(f"[Ring-{self._id}] Líder activo conocido, absorbiendo elección.")
                return
            with self._lock:
                self._in_election = True
                current_skip = list(self._suspected_dead_ids)
            target = self._get_next_target()
            logger.info(
                f"[Ring-{self._id}] Reenviando elección id={max_id} "
                f"(recibido={received_id}) → watchdog_{target} (skip={current_skip})."
            )
            self._send_to(target, {"tipo": "eleccion", "id": max_id, "skip": current_skip})

    def _declare_leader(self):
        with self._lock:
            if self._is_leader:
                logger.debug(f"[Ring-{self._id}] Ya soy líder, ignorando declaración duplicada.")
                return
            self._is_leader = True
            self._leader_id = self._id
            self._in_election = False
            self._election_started_at = None
            self._forwarded_coordinators.clear()
            coord_target = self._compute_next_target()
            self._suspected_dead_ids.pop(self._id, None)
            now = time.time()
            dead_nodes = [
                nid for nid, ts in self._suspected_dead_ids.items()
                if now - ts < self._config.suspected_dead_ttl
            ]
            self._became_leader_at = now
            self._standby_last_seen.clear()
            self._reported_dead_standbys.clear()

        logger.info(f"[Ring-{self._id}] ¡SOY EL LÍDER! Nodos caídos detectados: {dead_nodes}")
        self._on_become_leader(dead_nodes)
        self._send_to(coord_target, {"tipo": "coordinador", "id": self._id})
        threading.Thread(
            target=self._leader_hb_loop, daemon=True, name=f"ring-hb-send-{self._id}"
        ).start()

    def _handle_coordinator(self, leader_id: int):
        if leader_id == self._id:
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
            self._leader_id = leader_id
            self._last_leader_hb = time.time()
            self._in_election = False
            self._election_started_at = None
            self._suspected_dead_ids.pop(leader_id, None)
            was_leader = self._is_leader
            self._is_leader = False
            if was_leader:
                self._became_leader_at = None
                self._standby_last_seen.clear()
                self._reported_dead_standbys.clear()

        logger.info(f"[Ring-{self._id}] Líder establecido: watchdog_{leader_id}")

        if was_leader:
            self._on_lose_leader()

        if not already_forwarded:
            target = self._get_next_target()
            self._send_to(target, {"tipo": "coordinador", "id": leader_id})

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
            skip = payload.get("skip", [])
            ack()
            if tipo == "eleccion":
                self._handle_election(received_id, skip)
            elif tipo == "coordinador":
                self._handle_coordinator(received_id)
            elif tipo == "vivo":
                self._handle_alive(received_id)
            elif tipo == "hb_standby":
                self._handle_standby_hb(received_id)
            else:
                logger.warning(f"[Ring-{self._id}] Tipo de mensaje desconocido: {tipo}")
        except Exception as e:
            logger.warning(f"[Ring-{self._id}] Mensaje de anillo malformado: {e}")
            ack()

    # ------------------------------------------------------------------
    # Hilo: heartbeat sender del líder → standbys (fanout)
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
    # Hilo: consumidor de heartbeats del líder (para standbys)
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
                    self._suspected_dead_ids.pop(leader_id, None)
            logger.debug(f"[Ring-{self._id}] Heartbeat recibido de líder watchdog_{leader_id}.")
            ack()
        except Exception as e:
            logger.warning(f"[Ring-{self._id}] Heartbeat de líder malformado: {e}")
            ack()

    # ------------------------------------------------------------------
    # Hilo periódico unificado: timeout líder + HB standby + monitoreo standbys
    # ------------------------------------------------------------------

    def _periodic_loop(self):
        startup_time = time.time()
        while not self._stop_event.wait(self._config.check_leader_interval):
            self._tick_leader_timeout(startup_time)
            self._tick_standby_hb()
            self._tick_standbys_check()

    def _tick_leader_timeout(self, startup_time: float):
        with self._lock:
            if self._is_leader:
                return
            last_hb = self._last_leader_hb
            dead_leader = self._leader_id
            in_election = self._in_election
            election_started = self._election_started_at

        elapsed = (
            time.time() - last_hb
            if last_hb is not None
            else time.time() - startup_time
        )

        if elapsed <= self._config.leader_timeout_seconds:
            return

        election_timed_out = (
            in_election
            and election_started is not None
            and time.time() - election_started >= self._config.election_timeout
        )
        if in_election and not election_timed_out:
            return

        with self._lock:
            if dead_leader is not None:
                self._suspected_dead_ids[dead_leader] = time.time()
            self._leader_id = None
            self._last_leader_hb = None
            self._forwarded_coordinators.clear()

        logger.warning(
            f"[Ring-{self._id}] Sin heartbeat de líder por {elapsed:.1f}s. "
            "Iniciando nueva elección."
        )
        self._initiate_election()

    def _tick_standby_hb(self):
        with self._lock:
            is_leader = self._is_leader
            leader_id = self._leader_id
        if not is_leader and leader_id is not None:
            try:
                self._send_to(leader_id, {"tipo": "hb_standby", "id": self._id})
                logger.debug(
                    f"[Ring-{self._id}] Heartbeat de standby enviado al líder watchdog_{leader_id}."
                )
            except Exception as e:
                logger.warning(f"[Ring-{self._id}] Error enviando heartbeat de standby: {e}")

    def _tick_standbys_check(self):
        with self._lock:
            if not self._is_leader or self._became_leader_at is None:
                return
            now = time.time()
            if now - self._became_leader_at < self._config.leader_timeout_seconds:
                return
            dead = []
            for nid in range(1, self._n + 1):
                if nid == self._id or nid in self._reported_dead_standbys:
                    continue
                last_ts = self._standby_last_seen.get(nid)
                if last_ts is None or now - last_ts > self._config.leader_timeout_seconds:
                    dead.append(nid)
                    self._reported_dead_standbys.add(nid)

        for nid in dead:
            logger.warning(
                f"[Ring-{self._id}] Standby watchdog_{nid} sin heartbeat. Publicando caída."
            )
            if self._on_standby_dead is not None:
                self._on_standby_dead(nid)

    def _handle_standby_hb(self, node_id: int):
        with self._lock:
            if not self._is_leader:
                return
            self._standby_last_seen[node_id] = time.time()
            self._reported_dead_standbys.discard(node_id)
        logger.debug(f"[Ring-{self._id}] Heartbeat de standby recibido de watchdog_{node_id}.")

    # ------------------------------------------------------------------
    # Helpers de envío y routing
    # ------------------------------------------------------------------

    def _compute_next_target(self) -> int:
        """Calcula el próximo nodo del anillo saltando los sospechados. Requiere self._lock."""
        now = time.time()
        dead = {
            nid for nid, ts in self._suspected_dead_ids.items()
            if now - ts < self._config.suspected_dead_ttl
        }
        target = self._next_id
        for _ in range(self._n):
            if target not in dead:
                return target
            target = (target % self._n) + 1
        return self._next_id  # fallback: todos sospechados

    def _get_next_target(self) -> int:
        with self._lock:
            return self._compute_next_target()

    def _send_to(self, target_id: int, payload: dict):
        queue_name = f"ring.{target_id}"
        data = json.dumps(payload).encode()
        with self._send_lock:
            try:
                if target_id not in self._ring_senders:
                    self._ring_senders[target_id] = MessageMiddlewareQueueRabbitMQ(
                        self._config.mom_host, queue_name
                    )
                self._ring_senders[target_id].send(data)
            except Exception as e:
                logger.error(
                    f"[Ring-{self._id}] Error enviando {payload.get('tipo')} a {queue_name}: {e}",
                    exc_info=True,
                )
                self._ring_senders.pop(target_id, None)
                try:
                    self._ring_senders[target_id] = MessageMiddlewareQueueRabbitMQ(
                        self._config.mom_host, queue_name
                    )
                    self._ring_senders[target_id].send(data)
                except Exception as e2:
                    logger.error(
                        f"[Ring-{self._id}] Retry fallido enviando a {queue_name}: {e2}",
                        exc_info=True,
                    )
