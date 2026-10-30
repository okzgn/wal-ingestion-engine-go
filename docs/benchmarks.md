# Benchmarks

Medidos con `tools/loadgen` contra el binario, en la misma máquina (Windows 11, disco local, `fsync` real).
Comando: `loadgen -url http://127.0.0.1:<puerto>/webhook -n <N> -c <C> -size <B>`

## Línea base: código original (commit `71af138`)

| Concurrencia | Tamaño | Peticiones | Throughput | p50 | p99 |
|---:|---:|---:|---:|---:|---:|
| 1 | 200 B | 100 | **10 req/s** | 100 ms | 100.6 ms |
| 64 | 200 B | 20 000 | **632 req/s** | 99.9 ms | 186 ms |
| 64 | 4 KB | 10 000 | **645 req/s** | 99.4 ms | 147 ms |
| 256 | 200 B | 10 000 | **41 522 req/s** | 4.3 ms | 28 ms |

Lectura: el motor original solo hace flush al llegar a 100 mensajes (`MaxBatchSize`) o cuando salta
el ticker de 100 ms. Con poca concurrencia **cada petición espera ~100 ms** y el techo es
`concurrencia / 0.1 s`. Con 256 conexiones el lote se llena enseguida y el ticker deja de importar.
Este es el hallazgo **M2** y el objetivo principal de la Fase 5 (group commit adaptativo).

Nota: con concurrencia 512 el *cliente* `loadgen` agotó el límite de hilos del runtime en esta
máquina Windows (no es un fallo del servidor); los valores altos se miden hasta 256.

## Después de los cambios (rama `fix/hardening`)

Mismas cargas, misma máquina, `SINK_URL` vacío (el sink no influye en la ruta de ingesta).

| Concurrencia | Tamaño | Peticiones | Throughput | p50 | p99 | Antes (req/s) | Mejora |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 200 B | 100 | **1 758 req/s** | 0.52 ms | 1.0 ms | 10 | **x176** |
| 64 | 200 B | 20 000 | **37 705 req/s** | 1.6 ms | 3.3 ms | 632 | **x60** |
| 64 | 4 KB | 10 000 | **17 897 req/s** | 3.1 ms | 6.9 ms | 645 | **x28** |
| 256 | 200 B | 10 000 | **48 621 req/s** | 3.8 ms | 29.5 ms | 41 522 | x1.2 |

Qué cambió (hallazgo **M2**): ya no hay un ticker de 100 ms. El escritor hace *group commit
adaptativo*: bloquea hasta el primer mensaje, recoge sin esperar todo lo que haya en cola, escribe el
lote con un solo `write` y hace un solo `fsync`. Mientras corre el `fsync` se acumulan los siguientes
mensajes, así que el tamaño del lote se ajusta solo a la carga y la latencia a baja carga cae de
100 ms a ~0.5 ms. A alta carga (256 conexiones) el viejo diseño ya llenaba el lote de 100, por eso
ahí la ganancia es pequeña.

Micro-benchmark del escritor (`go test -bench Append -benchmem ./internal/wal`, 12 hilos,
mensaje de 68 B, archivos reales): **~3.8 µs/op, 128 B/op, 2 allocs/op** (**M8**: buffers de
mensaje del `sync.Pool`, un único buffer de lote reutilizado).

Caveat: el `fsync` de este disco es rápido; en un disco con `fsync` lento los números absolutos
bajarán, pero el *group commit* amortiza ese coste entre todas las peticiones concurrentes.

## Variación entre ejecuciones

Los números varían ±25 % de una ejecución a otra en esta máquina (por ejemplo, con 1 conexión se midieron
entre ~1 000 y ~1 750 req/s, y con 64 conexiones entre ~30 000 y ~38 000 req/s tras actualizar las
dependencias). El orden de magnitud y la comparación con la línea base se mantienen.

Cuidado al repetir pruebas de carga en Windows: tras decenas de miles de conexiones quedan miles de sockets
en `TIME_WAIT` y se agotan los puertos efímeros. Eso produce paradas de ~10 s y errores de red en el
*cliente* con 256 conexiones que no tienen nada que ver con el servidor (se comprobó: con los sockets
limpios, el mismo binario hace ~45 000 req/s sin errores, igual que el anterior a la actualización).
