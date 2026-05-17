# TP1_Distribuidos
Para ejecutar el proyecto, seguí estos pasos:

- Cloná el repositorio en tu máquina local.
- Prepará el dataset: Creá la carpeta `datasets` y colocá tu archivo CSV ahí.
- Configurá la topología: Modificá el archivo `config.json `definiendo tus grupos de workers, cantidad de réplicas y reglas de filtrado (como Amount Paid con operador lt).
- Generá el Compose: Ejecutá `make generar` en la terminal para compilar el nuevo archivo docker-compose.yml basado en tu configuración.
- Iniciá el backend: Ejecutá `make gateway` en una terminal para levantar RabbitMQ, el Gateway y los workers distribuidos.
- Ejecutá el cliente: En otra terminal, ejecutá `make client` para iniciar la transmisión masiva de datos y recibir los reportes filtrados en tiempo real.

(Opcional) Para ver los logs de cada worker hace `docker compose logs -f <nombre_worker>` (ejemplo: `docker compose logs -f filter_usd_1`)