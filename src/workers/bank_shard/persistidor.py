import os
import json
import logging
from common.persistencia import PersistidorEstado

logger = logging.getLogger(__name__)

class PersistidorEstadoShard:
    """
    Abstracción sobre el persistidor genérico de disco para manejar
    la serialización específica de bytes/estructuras del bank shard,
    segmentada por client_id.
    """
    def __init__(self, nombre_nodo: str):
        self.nombre_nodo = nombre_nodo
        self.base_dir = "/app/volumen"

    def _get_persistidor_cliente(self, client_id: str) -> PersistidorEstado:
        cid = str(client_id).strip()
        return PersistidorEstado(f"{self.nombre_nodo}/cliente_{cid}", base_dir=self.base_dir)

    def cargar_estado_cliente(self, client_id: str) -> tuple:
        client_id_str = str(client_id).strip()
        persistidor = self._get_persistidor_cliente(client_id_str)
        estado_cargado = persistidor.cargar()
        
        estado_agregador_raw = estado_cargado.get("estado_agregador", {})
        estado_agregador = {}
        for cid, val in estado_agregador_raw.items():
            estado_agregador[str(cid).strip()] = val
            
        estado_eof_raw = estado_cargado.get("estado_eof", {})
        
        estado_eof = {}
        for cid, datos in estado_eof_raw.items():
            cid_str = str(cid).strip()
            eof_mensaje = None
            if datos.get("eof_mensaje_payload"):
                eof_mensaje = json.dumps(datos["eof_mensaje_payload"]).encode('utf-8')
            
            estado_eof[cid_str] = {
                "transacciones_cerrado": datos.get("transacciones_cerrado", False),
                "bancos_cerrado": datos.get("bancos_cerrado", False),
                "flush_iniciado": datos.get("flush_iniciado", False),
                "eof_mensaje": eof_mensaje,
                "eof_mensaje_payload": datos.get("eof_mensaje_payload")
            }
        return estado_agregador, estado_eof

    def guardar_estado_cliente(self, client_id: str, estado_agregador: dict, estado_eof: dict) -> bool:
        client_id_str = str(client_id).strip()
        persistidor = self._get_persistidor_cliente(client_id_str)
        
        estado_eof_serializable = {}
        for cid, datos in estado_eof.items():
            cid_str = str(cid).strip()
            payload_msg = datos.get("eof_mensaje_payload")
            if not payload_msg and datos.get("eof_mensaje"):
                try:
                    payload_msg = json.loads(datos["eof_mensaje"].decode('utf-8'))
                except:
                    pass
            
            estado_eof_serializable[cid_str] = {
                "transacciones_cerrado": datos["transacciones_cerrado"],
                "bancos_cerrado": datos["bancos_cerrado"],
                "flush_iniciado": datos["flush_iniciado"],
                "eof_mensaje_payload": payload_msg
            }

        estado_completo = {
            "estado_agregador": estado_agregador,
            "estado_eof": estado_eof_serializable
        }
        return persistidor.guardar(estado_completo)

    def borrar_estado_cliente(self, client_id: str) -> bool:
        client_id_str = str(client_id).strip()
        persistidor = self._get_persistidor_cliente(client_id_str)
        return persistidor.borrar()

    def detectar_clientes_pendientes(self) -> list:
        dir_nodo = os.path.join(self.base_dir, self.nombre_nodo)
        if not os.path.exists(dir_nodo):
            return []
        
        clientes = []
        try:
            for item in os.listdir(dir_nodo):
                full_path = os.path.join(dir_nodo, item)
                if os.path.isdir(full_path) and item.startswith("cliente_"):
                    client_id = item.replace("cliente_", "")
                    if client_id:
                        clientes.append(client_id)
        except Exception as e:
            logger.error(f"[PersistidorShard] Error detectando clientes pendientes: {e}")
        return clientes

