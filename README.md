Tarea 1: Sistemas Distribuidos 

Descripción: 
Este proyecto implementa una arquitectura de servicios utilizando Docker para simular la gestión de consultas geográficas sobre un dataset de edificios en Santiago, este sistema utiliza una capa de caché con Redis para optimizar los tiempos de respuesta y persiste métricas de rendimiento en una base de datos SQLite.

Arquitectura
El sistema se compone de 4 servicios principales:

Traffic Generator: Genera 1000 consultas automáticas bajo distribuciones Zipf y Uniforme.

Response Generator: Procesa las consultas sobre el dataset filtrado (santiago_buildings.csv).

Redis Cache: Almacena los resultados de consultas frecuentes para reducir la latencia.

Metrics Service: Almacena estadísticas de Hit Rate y latencia (p50, p95) en SQLite.

Instalación y Ejecución:

Requisitos previos:

Docker y Docker Compose instalado.

Dataset santiago_buildings.csv en la carpeta /data.

Pasos para iniciar:

Clonar el repositorio:

git clone https://github.com/tu-usuario/tarea1-sd.git
cd tarea1-sd
Configurar la distribución:
Edita el archivo docker-compose.yml y ajusta la variable DISTRIBUTION entre zipf o uniform.

Levantar los servicios:

docker-compose up --build

Análisis de Resultados:
Los resultados de la simulación se guardan automáticamente en la carpeta /results:

traffic_results.json: Registro detallado de cada consulta (Hit/Miss y Latencia).

metrics.db: Base de datos SQLite con el histórico de eventos.

Comparativa de Rendimiento (Observada):

Latencia Promedio (Cache Hit): ~7ms.

Latencia Promedio (Cache Miss): ~70ms - 120ms.

Efectividad: El uso de Redis permite una reducción de latencia de aproximadamente un 90%.
