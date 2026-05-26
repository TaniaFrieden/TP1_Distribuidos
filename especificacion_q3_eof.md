# Especificación de coordinación EOF para Q3

## Objetivo

Quiero que Q3 quede implementada con una coordinación por fases, donde el comportamiento de EOF sea distinto para la cola de temprano y para la cola de tardío.

La idea central es esta:
- El EOF de temprano habilita el cálculo de promedios.
- El EOF de tardío habilita el cierre final de la etapa de formato.
- El flush global no debe ocurrir cuando llega el primer EOF, sino cuando están cerradas ambas fases y ya no quedan mensajes en vuelo.
- El orden de llegada de los EOF no importa.

## Contexto actual

Hoy la base del worker trata EOF de forma genérica por cliente y por cola local. Cuando llegan todos los EOF locales, se dispara una barrera distribuida. Eso funciona para pipelines simples, pero no expresa bien la lógica de Q3, porque Q3 tiene dos fases semánticamente distintas:
- temprano: se usan para calcular promedios
- tardío: se cachean hasta que ya existen los promedios y luego se procesan

El worker final de Q3 consume dos colas locales por cliente:
- q3_temprano_{id}
- q3_tardio_{id}

Por eso no alcanza con un único estado EOF local genérico.

## Comportamiento deseado

### Fase temprano

Cuando llega EOF por una cola de temprano:
- se marca que la fase temprano cerró para ese client_id
- se toma todo lo acumulado en temprano y se calculan los promedios
- se libera la dependencia lógica sobre temprano
- no se hace flush global todavía
- si ya había llegado EOF de tardío, se habilita el procesamiento final del cache tardío

### Fase tardío

Mientras llegan mensajes por una cola de tardío:
- se guardan en memoria en orden de llegada
- no se procesan hasta que los promedios estén disponibles

Cuando llega EOF por una cola de tardío:
- se marca que la fase tardío cerró para ese client_id
- si los promedios ya están calculados, se procesa todo el cache tardío
- si todavía no están los promedios, se espera a que llegue EOF de temprano
- recién cuando ambas fases están cerradas se pasa a la coordinación con el resto de nodos para el flush

### Regla de orden

El orden de llegada de los EOF no debe cambiar la semántica:
- si llega primero temprano, se calculan promedios y se espera tardío
- si llega primero tardío, se cachea y se espera temprano
- si llega primero cualquiera de los dos, no se pierde información
- el flush final solo puede ocurrir cuando ambas fases están listas

## Qué hay que cambiar

### 1. Separar la semántica de EOF por cola

La coordinación actual necesita distinguir la cola que entregó el EOF.

La implementación debe poder saber, para cada mensaje EOF:
- client_id
- queue_name
- si la cola pertenece a temprano o a tardío

Eso es necesario para decidir qué bandera de estado actualizar.

### 2. Mantener estado por client_id y por fase

El worker de Q3 final debería manejar un estado por cliente con, como mínimo, estas piezas:
- eof_temprano_recibido
- eof_tardio_recibido
- promedios_calculados
- cache_tardio
- datos_temprano acumulados
- indicador de si ya se ejecutó el procesamiento final

Ese estado tiene que evitar duplicar flushes y evitar que una fase se procese dos veces.

### 3. Calcular promedios en el EOF de temprano

Al cerrar temprano:
- no se hace flush
- se calculan promedios usando lo acumulado en temprano
- esos promedios quedan disponibles para procesar los mensajes tardíos ya cacheados o los que lleguen después, según corresponda

### 4. Procesar cache tardío cuando ya hay promedios

Cuando ambas condiciones se cumplan:
- temprano cerrado
- tardío cerrado
- promedios calculados

entonces se recorre el cache de tardío y se emiten los resultados finales.

### 5. Hacer flush global solo una vez

El flush distribuido no debe dispararse en el primer EOF local.
Debe dispararse solo cuando:
- se cerró temprano
- se cerró tardío
- el cache tardío ya fue procesado
- el worker local terminó su parte

En ese punto recién se avisa al coordinador general del nodo para entrar en la barrera distribuida.

## Cambios de diseño recomendados

### En la base del worker

La base actual trata EOF de forma demasiado general para este caso.

Se recomienda una de estas dos opciones:
- Opción A: pasar el nombre de la cola al worker de forma explícita y dejar que la subclase decida qué hacer con cada EOF.
- Opción B: agregar un hook específico por fase, para que Q3 pueda distinguir EOF de temprano y EOF de tardío antes de entrar en la lógica genérica.

La opción más simple para implementar es que el worker final de Q3 reciba el queue_name y resuelva internamente la fase.

### En el worker de Q3

El worker final debe dejar de depender de un único flujo genérico de cierre.
Necesita una máquina de estados por client_id.

Estados sugeridos:
- esperando_datos
- temprano_cerrado
- tardio_cerrado
- promedios_listos
- cache_procesado
- flush_enviado

Transiciones sugeridas:
- si llega un mensaje normal de temprano, acumular suma y contador
- si llega EOF de temprano, marcar temprano_cerrado y calcular promedios
- si llega un mensaje normal de tardío, cachearlo
- si llega EOF de tardío, marcar tardio_cerrado
- si temprano_cerrado y tardio_cerrado y promedios_listos, procesar cache y disparar coordinación de flush

### En el coordinador

El coordinador global no debería asumir que cualquier EOF local implica flush inmediato.
Debe esperar la señal de la fase correcta del worker final.

Si se mantiene la barrera distribuida actual, el worker final solo debería llamar a esa barrera cuando el procesamiento local de Q3 ya esté listo para cerrar.

## Qué no debe pasar

- No debe hacerse flush al llegar el EOF de temprano solamente.
- No debe perderse el cache tardío si llega antes que temprano.
- No debe ejecutarse el flush dos veces para el mismo client_id.
- No debe depender del orden de llegada de las dos colas.
- No debe seguir existiendo una espera artificial con timers para resolver la coordinación.

## Criterios de aceptación

La implementación se considera correcta si cumple todo esto:
- Q3 procesa mensajes de temprano y tardío aunque los EOF lleguen en cualquier orden.
- El EOF de temprano habilita el cálculo de promedios, no el flush.
- El EOF de tardío cierra la etapa de cache y habilita el cierre final.
- El worker final procesa el cache tardío solo cuando ya existen promedios.
- La coordinación global se dispara una sola vez por client_id.
- No hay sleeps arbitrarios ni polling temporal para resolver el cierre.
- El comportamiento con una sola query y con múltiples queries sigue siendo consistente.

## Validaciones esperadas

Se debería probar al menos esto:
- caso 1: llega primero temprano EOF y después tardío EOF
- caso 2: llega primero tardío EOF y después temprano EOF
- caso 3: ambos EOF llegan con intercalado de mensajes en vuelo
- caso 4: Q3 sola funciona
- caso 5: Q2 y Q3 juntas no se pisan en sus colas ni en el cierre global

## Resumen para implementar

Si otra IA lo implementa, tiene que hacer esto:
- distinguir EOF por cola y por fase
- guardar estado por client_id
- calcular promedios cuando cierra temprano
- cachear tardíos hasta que también cierre esa fase
- disparar flush global solo cuando ambas fases terminaron
- eliminar esperas artificiales basadas en tiempo