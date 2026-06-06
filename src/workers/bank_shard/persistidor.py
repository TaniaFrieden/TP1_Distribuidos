import json
from common.persistencia import PersistidorEstado

class PersistidorEstadoShard:
    """
    Abstracción sobre el persistidor genérico de disco para manejar
    la serialización específica de bytes/estructuras del bank shard.
    """
    def __init__(self, nombre_nodo: str):
        self.persistidor = PersistidorEstado(nombre_nodo)

    def cargar_estado(self) -> tuple:
        estado_cargado = self.persistidor.cargar()
        estado_agregador = estado_cargado.get("estado_agregador", {})
        estado_eof_raw = estado_cargado.get("estado_eof", {})
        
        estado_eof = {}
        for client_id, datos in estado_eof_raw.items():
            eof_mensaje = None
            if datos.get("eof_mensaje_payload"):
                eof_mensaje = json.dumps(datos["eof_mensaje_payload"]).encode('utf-8')
            
            estado_eof[client_id] = {
                "transacciones_cerrado": datos.get("transacciones_cerrado", False),
                "bancos_cerrado": datos.get("bancos_cerrado", False),
                "flush_iniciado": datos.get("flush_iniciado", False),
                "eof_mensaje": eof_mensaje,
                "eof_mensaje_payload": datos.get("eof_mensaje_payload")
            }
        return estado_agregador, estado_eof

    def guardar_estado(self, estado_agregador: dict, estado_eof: dict):
        estado_eof_serializable = {}
        for client_id, datos in estado_eof.items():
            payload_msg = datos.get("eof_mensaje_payload")
            if not payload_msg and datos.get("eof_mensaje"):
                try:
                    payload_msg = json.loads(datos["eof_mensaje"].decode('utf-8'))
                except:
                    pass
            
            estado_eof_serializable[client_id] = {
                "transacciones_cerrado": datos["transacciones_cerrado"],
                "bancos_cerrado": datos["bancos_cerrado"],
                "flush_iniciado": datos["flush_iniciado"],
                "eof_mensaje_payload": payload_msg
            }

        estado_completo = {
            "estado_agregador": estado_agregador,
            "estado_eof": estado_eof_serializable
        }
        self.persistidor.guardar(estado_completo)
