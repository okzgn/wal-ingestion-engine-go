# Regresiones reproducidas en el código original

Cada escenario se reprodujo contra el commit `71af138` y se convierte en un test automático
en el rediseño. Mientras no pasen todos, el hallazgo sigue abierto.

| ID | Escenario | Comportamiento original | Comportamiento esperado | Test |
|---|---|---|---|---|
| C2 | `POST` con JSON multilínea (`{\n "id": 2\n}`) | `200`, el mensaje se descarta al procesar | `200` y se entrega **una** línea JSON válida | `TestHandler_MultilineStoredAsOneLine` |
| C2 | `POST` con `not json` o cuerpo vacío | `200`, se descarta en silencio | `400` | `TestHandler_InvalidBodyIs400` |
| C1 | Sink falla en la línea 3 de 6 | Se borra el archivo; líneas 4–6 perdidas; checkpoint = 3 | Archivo y checkpoint conservados; las 6 se entregan al recuperarse | `TestWorker_SinkFailureKeepsFileAndCheckpoint` |
| C3 | `POST` de 3 MB | `200`; `token too long`; `os.Create` trunca el archivo a 0 B (Windows) | `413` | `TestHandler_Oversize413` |
| C3 | Línea gigante dentro de un archivo | Aborta el archivo completo | Solo esa línea va a `failed/*.bad` | `TestLineReader_OversizeLineQuarantined` |
| C4 | `Sync`/`Flush` devuelven error | ACK `200` igualmente | Todos los pendientes reciben `503` | `TestWriter_SyncErrorNacksAll` |
| A2 | No se puede crear el archivo siguiente | `log.Fatalf` (el proceso muere) | Estado degradado, `503`, recuperación automática | `TestWriter_OpenFailureDegrades` |
| A1 | Arranque con archivos en `ingestions/` y `processing/` | Carrera: un archivo puede encolarse dos veces | Cada archivo se procesa una sola vez | `TestSpool_RecoverNoDuplicates` |
