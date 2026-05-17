import os
import logging
import csv
import json
import socket
import signal

from common import message_protocol

INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]
SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "100"))


class Client:

    def __init__(self):
        self.closed = False
        self.server_socket = None
        self._prev_sigterm_handler = signal.signal(signal.SIGTERM, self.handle_sigterm)

    def handle_sigterm(self, signum, frame):
        logging.info("Recibida señal SIGTERM")
        self.closed = True
        self.disconnect()
        if self._prev_sigterm_handler:
            self._prev_sigterm_handler(signum, frame)

    def connect(self, server_host, server_port):
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.connect((server_host, server_port))
        logging.info(f"Conectado a {server_host}:{server_port}")

    def disconnect(self):
        if self.server_socket:
            try:
                self.server_socket.shutdown(socket.SHUT_RDWR)
            except:
                pass
            self.server_socket.close()

    def send_csv_in_batches(self, input_file, batch_size=100):
        """Lee el CSV y envía las transacciones en lotes."""
        logging.info(f"Leyendo transacciones desde {input_file}")
        
        batch = []
        # Usar DictReader para manejar cabeceras y enviar la fila completa.
        with open(input_file, newline="") as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

            # Validar que exista la columna esperada
            if not csv_reader.fieldnames or "Amount Received" not in csv_reader.fieldnames:
                raise ValueError("CSV missing 'Amount Received' column")

            for row in csv_reader:
                try:
                    # Parsear el monto recibido como float (siempre que exista)
                    raw_amount = (row.get("Amount Received") or "").strip()
                    amount = float(raw_amount) if raw_amount != "" else 0.0

                    # Añadir el valor parseado a la fila para facilidad del servidor
                    # row["_AmountReceivedParsed"] = amount

                    # Enviar la fila completa (como dict) en el lote
                    batch.append(row)

                    # Enviar lote cuando alcanza el tamaño
                    if len(batch) >= batch_size:
                        self._send_batch(batch)
                        batch = []
                except ValueError as e:
                    logging.warning(f"Fila inválida: {row} - {e}")
                    continue
            
            # Enviar último lote si queda algo
            if batch:
                self._send_batch(batch)
        
        # Señalizar fin de transmisión
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.END_OF_RECODS
        )
        logging.info("Fin de registros enviado")

    def _send_batch(self, batch):
        """Envía un lote de registros."""
        logging.info(f"Enviando lote de {len(batch)} registros")
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.LOTE,
            batch
        )
        # Esperar ACK del servidor
        msg_type, _ = message_protocol.external.recv_msg(self.server_socket)
        if msg_type != message_protocol.external.MsgType.ACK:
            raise RuntimeError(f"Esperaba ACK, recibí tipo {msg_type}")

    def recv_reporte(self, output_file):
        """Recibe el reporte del servidor y lo guarda."""
        logging.info("Esperando reporte del servidor...")
        msg_type, reporte = message_protocol.external.recv_msg(self.server_socket)
        
        if msg_type != message_protocol.external.MsgType.REPORTE:
            raise RuntimeError(f"Esperaba REPORTE, recibí tipo {msg_type}")
        
        # Guardar reporte
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        self._save_report_as_csv(reporte, output_file)
        logging.info(f"Reporte guardado en {output_file}")
        
        # Enviar ACK
        message_protocol.external.send_msg(
            self.server_socket,
            message_protocol.external.MsgType.ACK
        )

    def _save_report_as_csv(self, reporte, output_file):
        """Convierte el reporte JSON (lista de dicts) a CSV."""
        try:
            parsed_report = json.loads(reporte)
        except json.JSONDecodeError as exc:
            raise RuntimeError("El reporte recibido no es JSON válido") from exc

        if not isinstance(parsed_report, list):
            raise RuntimeError("El reporte recibido no tiene formato de lista")

        if not parsed_report:
            with open(output_file, "w", newline="") as csvfile:
                csvfile.write("")
            return

        if not all(isinstance(row, dict) for row in parsed_report):
            raise RuntimeError("El reporte no es una lista de registros")

        fieldnames = list(parsed_report[0].keys())
        with open(output_file, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(parsed_report)


def main() -> int:
    logging.basicConfig(level=logging.INFO)
    client = Client()

    try:
        client.connect(SERVER_HOST, SERVER_PORT)
        client.send_csv_in_batches(INPUT_FILE, BATCH_SIZE)
        client.recv_reporte(OUTPUT_FILE)
    except socket.error as e:
        if not client.closed:
            logging.error(f"Conexión perdida: {e}")
            return 1
    except FileNotFoundError as e:
        logging.error(f"Archivo no encontrado: {e}")
        return 2
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
        return 3
    finally:
        if not client.closed:
            client.disconnect()

    return 0


if __name__ == "__main__":
    exit(main())
