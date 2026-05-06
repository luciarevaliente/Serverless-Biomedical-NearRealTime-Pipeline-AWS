# TODO

## Infraestructura
- [ ] Añadir cloudtrail.tf al módulo biomedical_pipeline

## Código Python
- [ ] Escribir handler.py completo (Lambda):
  - [ ] Validación de campos obligatorios
  - [ ] Conversión de unidades
  - [ ] Enriquecimiento con timestamps
  - [ ] Escritura de datos crudos en S3
  - [ ] Escritura de datos procesados en DynamoDB
  - [ ] Envío de eventos fallidos a la DLQ
  - [ ] Envío de métricas a CloudWatch (PipelineLatencyMs, ProcessedEvents, IngestedEvents, StoredEvents)

## Simulador de datos
- [ ] Escribir simulator.py:
  - [ ] Generar eventos siguiendo el modelo de datos del TFG
  - [ ] Simular frecuencias de muestreo heterogéneas (4 Hz, 32 Hz, 64 Hz, 700 Hz)
  - [ ] Enviar eventos a Kinesis
  - [ ] Configurar los 3 escenarios experimentales (base, carga sostenida, pico)

## Experimentos
- [ ] Ejecutar escenario base (8 ev/s, 5 min × 3 repeticiones)
- [ ] Ejecutar escenario de carga sostenida (208 ev/s, 5 min × 3 repeticiones)
- [ ] Ejecutar escenario de pico (4.304 ev/s, 30s × 3 repeticiones)
- [ ] Recoger métricas de CloudWatch
- [ ] Calcular P95 de latencia end-to-end
- [ ] Verificar integridad de datos (IngestedEvents vs StoredEvents)
- [ ] Calcular coste real de la sesión experimental

## Al terminar
- [ ] terraform destroy para eliminar la infraestructura y dejar de pagar