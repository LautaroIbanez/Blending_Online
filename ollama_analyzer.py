import requests
import json
import os
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import logging
import time
from requests.exceptions import Timeout
import hashlib
from functools import lru_cache
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class NumpyEncoder(json.JSONEncoder):
    """Encoder personalizado para manejar tipos de datos de NumPy."""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)

def preprocess_data(data):
    """Preprocesa los datos reduciendo significativamente su volumen y mejorando su legibilidad."""
    if data.empty:
        logging.warning("DataFrame vacío recibido en preprocess_data")
        return {"summary": "No hay datos disponibles."}
    
    # Imprimir información de depuración
    logging.debug(f"Columnas disponibles en el DataFrame: {data.columns.tolist()}")
    
    summary = {}
    
    # Verificar si es un DataFrame de producción o de sensores
    is_production_data = 'Ora start' in data.columns and 'Ora stop' in data.columns
    is_sensor_data = 'Date [dd/mm/yyyy]' in data.columns and 'Time [hh:mm:ss]' in data.columns
    
    if is_production_data:
        # Procesar datos de producción
        summary['tipo'] = 'produccion'
        summary['ciclos'] = []
        
        for _, row in data.iterrows():
            ciclo = {
                'fecha_inicio': str(row['Ora start']),
                'fecha_fin': str(row['Ora stop']),
                'componentes': {}
            }
            
            # Procesar componentes
            componentes = {
                'Agua': ['Dosaggio Acqua [l/m³]', 'Acqua [l]'],
                'Cemento': ['Dosaggio Cemento [kg/m³]', 'Cemento [kg]'],
                'Inerte A': ['Dosaggio Inerte A [kg/m³]', 'Inerte A [kg]'],
                'Inerte B': ['Dosaggio Inerte B [kg/m³]', 'Inerte B [kg]'],
                'Additivo 1': ['Dosaggio Additivo 1 [l/m³]', 'Additivo 1 [l]'],
                'Additivo 3': ['Dosaggio Additivo 3 [l/m³]', 'Additivo 3 [l]']
            }
            
            for nombre, columnas in componentes.items():
                valores = {}
                for col in columnas:
                    if col in row and pd.notnull(row[col]):
                        valores[col] = float(row[col])
                if valores:
                    ciclo['componentes'][nombre] = valores
            
            summary['ciclos'].append(ciclo)
        
        # Agregar estadísticas generales
        if 'Produzione oraria [m³/h]' in data.columns:
            summary['produccion_promedio'] = float(data['Produzione oraria [m³/h]'].mean())
        if 'Calcestruzzo [m³]' in data.columns:
            summary['volumen_total'] = float(data['Calcestruzzo [m³]'].sum())
    
    elif is_sensor_data:
        # Procesar datos de sensores
        summary['tipo'] = 'sensores'
        
        # Encontrar los ciclos de producción
        productions = []
        if 'Event type' in data.columns:
            date_events = data['Event type'].tolist()
            hourly_events = data['Time [hh:mm:ss]'].tolist()
            
            try:
                start_indices = [i for i, event in enumerate(date_events) if event == 'Start cycle']
                stop_indices = [i for i, event in enumerate(date_events) if event == 'Stop cycle']
                
                if start_indices and stop_indices:
                    for start_idx in start_indices:
                        # Encontrar el siguiente Stop cycle después de este Start cycle
                        next_stops = [stop_idx for stop_idx in stop_indices if stop_idx > start_idx]
                        if next_stops:
                            stop_idx = next_stops[0]
                            production_start = hourly_events[start_idx]
                            production_end = hourly_events[stop_idx]
                            if production_start > production_end:
                                production_start, production_end = production_end, production_start
                            productions.append((production_start, production_end))
                else:
                    logging.warning("No se encontraron eventos Start cycle o Stop cycle")
            except Exception as e:
                logging.error(f"Error al procesar ciclos de producción: {str(e)}")
        
        # Procesar cada ciclo de producción
        production_data = []
        for start_time, end_time in productions:
            # Filtrar datos para este ciclo de producción
            cycle_data = data[
                (data['Time [hh:mm:ss]'] >= start_time) & 
                (data['Time [hh:mm:ss]'] <= end_time)
            ]
            
            if not cycle_data.empty:
                production_info = {
                    'fecha': str(cycle_data['Date [dd/mm/yyyy]'].iloc[0]),
                    'hora_inicio': start_time,
                    'hora_fin': end_time,
                    'componentes': {}
                }
                
                # Componentes a monitorear
                components = {
                    'Water': ['Ev DV', 'Ev MV'],
                    'Cement': ['Ev DV', 'Ev MV'],
                    'Aggregate': ['Ev DV', 'Ev MV'],
                    'Additive': ['Ev DV', 'Ev MV']
                }
                
                # Procesar cada componente
                for component, metrics in components.items():
                    component_data = {}
                    for metric in metrics:
                        metric_col = f"{component} {metric}"
                        if metric_col in cycle_data.columns:
                            values = cycle_data[metric_col].dropna()
                            if not values.empty:
                                component_data[metric] = {
                                    'valor': round(float(values.iloc[-1]), 1),
                                    'media': round(float(values.mean()), 1),
                                    'std_dev': round(float(values.std()), 1) if len(values) > 1 else 0.0
                                }
                    
                    if component_data:
                        production_info['componentes'][component] = component_data
                
                production_data.append(production_info)
        
        summary['producciones'] = production_data
        
        # Agregar estadísticas generales
        summary['event_count'] = len(data)
        if 'Event type' in data.columns:
            summary['event_types'] = data['Event type'].value_counts().to_dict()
    
    else:
        logging.warning("Formato de datos no reconocido")
        return {"summary": "Formato de datos no reconocido"}
    
    # Imprimir información de depuración
    logging.debug(f"Resumen generado: {json.dumps(summary, indent=2)}")
    
    return summary

def preprocess_recent_events(df, key_cols=None):
    """Preprocesa eventos recientes con un enfoque más conciso."""
    if df.empty:
        return {"summary": "No hay datos recientes."}
    
    if key_cols is None:
        key_cols = [col for col in ['Ora start', 'Ora', 'Codice', 'Descrizione', 'Type', 'Event type'] 
                   if col in df.columns]
    
    # Reducir a las últimas 2 filas
    recent = df.tail(2)[key_cols]
    
    summary = {}
    
    # Resumir conteos de eventos (solo los 2 más frecuentes)
    for col in ['Event type', 'Type', 'Codice']:
        if col in df.columns:
            summary[f'{col}_counts'] = dict(df.tail(5)[col].value_counts().head(2))
    
    # Resumir registros recientes
    summary['recent_records'] = recent.astype(str).to_dict('records')
    
    return summary

def get_existing_columns(df, candidates):
    return [col for col in candidates if col in df.columns]

class OllamaAnalyzer:
    def __init__(self, model_name: str = "phi"):
        """
        Inicializa el analizador de Ollama.
        
        Args:
            model_name (str): Nombre del modelo a utilizar (default: "phi")
        """
        self.model_name = model_name
        self.base_url = "http://localhost:11434"
        self.timeout = 300  # 5 minutos
        self.max_retries = 5
        self.docs_dir = os.path.join(os.path.dirname(__file__), "docs")
        self.cache_dir = "analysis_cache"
        self._ensure_directories()
        self.machine_docs = self._load_machine_docs()
        self.executor = ThreadPoolExecutor(max_workers=1)
        logging.debug("OllamaAnalyzer initialized")
        
        # Verificar modelos disponibles y ajustar el modelo si es necesario
        self.model_name = self._get_available_model(self.model_name)
        
        # Verificar conexión y modelo
        try:
            self._check_ollama_connection()
        except Exception as e:
            logging.error(f"Error en la inicialización: {str(e)}")
            raise
    
    def _ensure_directories(self):
        """Asegura que los directorios necesarios existen."""
        for directory in [self.docs_dir, self.cache_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
                logging.info(f"Created directory: {directory}")

    def _load_machine_docs(self) -> Dict:
        """Carga la documentación de la máquina con mejor manejo de errores."""
        docs_file = os.path.join(self.docs_dir, "machine_docs.txt")
        default_docs = {
            "machine_info": {
                "type": "Planta de hormigón",
                "components": {
                    "silos": ["Cemento", "Áridos", "Aditivos"],
                    "mezclador": "Tipo planetario",
                    "sistema_dosificación": "Pesaje continuo",
                    "sistema_control": "PLC"
                },
                "maintenance": {
                    "daily": ["Limpieza general", "Verificación de niveles", "Comprobación de fugas"],
                    "weekly": ["Lubricación de componentes móviles", "Verificación de sensores"],
                    "monthly": ["Calibración de pesajes", "Revisión de mezclador", "Limpieza de filtros"]
                }
            },
            "common_issues": {
                "dosificacion": {
                    "symptoms": ["Variaciones en la dosificación", "Errores de pesaje"],
                    "causes": ["Descalibración", "Fugas", "Obstrucciones"],
                    "solutions": ["Recalibración", "Revisión de sellos", "Limpieza de conductos"]
                },
                "mezclador": {
                    "symptoms": ["Ruidos anormales", "Vibraciones excesivas"],
                    "causes": ["Desgaste de palas", "Desbalanceo", "Falta de lubricación"],
                    "solutions": ["Reemplazo de palas", "Balanceo", "Lubricación"]
                },
                "control": {
                    "symptoms": ["Errores de comunicación", "Lecturas anómalas"],
                    "causes": ["Interferencias", "Fallo de sensores", "Problemas de conexión"],
                    "solutions": ["Verificación de cableado", "Reemplazo de sensores", "Reinicio del sistema"]
                }
            },
            "diagnostic_procedures": {
                "general": [
                    "Verificar registros de eventos",
                    "Comprobar lecturas de sensores",
                    "Revisar alarmas activas"
                ],
                "specific": {
                    "dosificacion": [
                        "Verificar calibración de pesajes",
                        "Comprobar flujo de materiales",
                        "Revisar actuadores"
                    ],
                    "mezclador": [
                        "Verificar nivel de aceite",
                        "Comprobar tensión de correas",
                        "Revisar rodamientos"
                    ],
                    "control": [
                        "Verificar conexiones",
                        "Comprobar alimentación",
                        "Revisar logs del sistema"
                    ]
                }
            }
        }

        try:
            if not os.path.exists(docs_file):
                logging.warning(f"machine_docs.txt not found in {self.docs_dir}")
                # Crear archivo de documentación por defecto
                with open(docs_file, 'w', encoding='utf-8') as f:
                    json.dump(default_docs, f, indent=2)
                logging.info(f"Created default machine documentation at {docs_file}")
                return default_docs

            with open(docs_file, 'r', encoding='utf-8') as f:
                try:
                    docs = json.load(f)
                    logging.info("Successfully loaded machine documentation")
                    return docs
                except json.JSONDecodeError:
                    logging.error(f"Invalid JSON format in {docs_file}")
                    return default_docs

        except Exception as e:
            logging.error(f"Error loading machine documentation: {str(e)}")
            return default_docs
    
    def _check_ollama_connection(self) -> None:
        """Verifica la conexión con Ollama y la disponibilidad del modelo."""
        try:
            logging.debug("Checking Ollama connection...")
            response = requests.get(f"{self.base_url}/api/tags", timeout=30)
            
            if response.status_code == 200:
                models = response.json().get('models', [])
                available_models = [model['name'] for model in models]
                logging.info(f"Available Ollama models: {', '.join(available_models)}")
                
                # Verificar si el modelo solicitado está disponible
                if self.model_name not in available_models:
                    logging.warning(f"Model {self.model_name} not found. Available models: {available_models}")
                    
                    # Intentar encontrar un modelo alternativo
                    preferred_models = ["mistral", "llama2", "llama"]
                    for model in preferred_models:
                        if model in available_models:
                            self.model_name = model
                            logging.info(f"Using alternative model: {self.model_name}")
                            break
                    else:
                        if available_models:
                            self.model_name = available_models[0]
                            logging.info(f"No preferred model found. Using first available: {self.model_name}")
                        else:
                            raise ConnectionError("No models available in Ollama")
            else:
                raise ConnectionError(f"Error al conectar con Ollama: {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            logging.error("Could not connect to Ollama service")
            raise ConnectionError("Ollama no está ejecutándose. Por favor, inicia el servicio Ollama.")
        except requests.exceptions.Timeout:
            logging.error("Connection to Ollama timed out")
            raise ConnectionError("Timeout al conectar con Ollama. Por favor, verifica que el servicio esté respondiendo.")
        except Exception as e:
            logging.error(f"Unexpected error checking Ollama connection: {str(e)}")
            raise ConnectionError(f"Error inesperado al conectar con Ollama: {str(e)}")
    
    def _get_cache_key(self, data: Dict) -> str:
        """Genera una clave única para el caché basada en los datos."""
        try:
            data_str = json.dumps(data, sort_keys=True, cls=NumpyEncoder)
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception as e:
            logging.error(f"Error generating cache key: {str(e)}")
            return hashlib.md5(str(data).encode()).hexdigest()

    def _get_cached_result(self, cache_key: str) -> Optional[str]:
        """Obtiene un resultado del caché si existe."""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    if time.time() - cache_data['timestamp'] < 86400:  # 24 horas
                        return cache_data['result']
            except Exception as e:
                logging.warning(f"Error reading cache: {str(e)}")
        return None

    def _save_to_cache(self, cache_key: str, result: str):
        """Guarda un resultado en el caché."""
        cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
        try:
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': time.time(),
                    'result': result
                }, f, cls=NumpyEncoder)
        except Exception as e:
            logging.warning(f"Error saving to cache: {str(e)}")

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=retry_if_exception_type((Timeout, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout))
    )
    def _make_api_request(self, prompt: str, cache_key: Optional[str] = None) -> str:
        """Realiza una solicitud a la API de Ollama con manejo de errores y reintentos."""
        try:
            # Verificar si hay un resultado en caché
            if cache_key:
                cached_result = self._get_cached_result(cache_key)
                if cached_result:
                    logging.debug("Using cached result")
                    return cached_result

            # Preparar los datos de la solicitud
            request_data = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_predict": 1024,
                    "top_k": 40,
                    "top_p": 0.9
                }
            }

            # Realizar la solicitud con timeouts más largos
            logging.debug(f"Sending request to Ollama API with model: {self.model_name}")
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=request_data,
                timeout=(120, 300)  # (connect timeout, read timeout) - increased to 2 minutes connect, 5 minutes read
            )

            # Verificar la respuesta
            if response.status_code == 200:
                response_data = response.json()
                logging.debug(f"Received response from {self.model_name}")
                
                # Guardar en caché si se proporcionó una clave
                if cache_key:
                    self._save_to_cache(cache_key, response_data["response"])
                
                return response_data["response"]
            else:
                error_msg = f"Error en la solicitud a la API: {response.status_code} - {response.text}"
                logging.error(error_msg)
                raise Exception(error_msg)

        except requests.exceptions.Timeout:
            error_msg = f"Timeout al conectar con la API de Ollama usando modelo {self.model_name}. Por favor, verifica que el servicio esté respondiendo y que el modelo esté cargado correctamente."
            logging.error(error_msg)
            raise Timeout(error_msg)
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Error de conexión con la API de Ollama: {str(e)}. Por favor, verifica que el servicio Ollama esté ejecutándose."
            logging.error(error_msg)
            raise requests.exceptions.ConnectionError(error_msg)
        except Exception as e:
            error_msg = f"Error inesperado en la solicitud a la API: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)

    def _truncate_prompt(self, prompt: str) -> str:
        """Trunca el prompt manteniendo la información más relevante."""
        try:
            # Si el prompt es JSON, intentar truncar manteniendo la estructura
            data = json.loads(prompt)
            if isinstance(data, dict):
                # Mantener solo los campos más importantes y limitar valores
                important_fields = ['sensor_data', 'production_data']
                truncated_data = {}
                for field in important_fields:
                    if field in data:
                        field_data = data[field]
                        if isinstance(field_data, dict):
                            # Limitar a los primeros 3 elementos de cada campo
                            truncated_data[field] = dict(list(field_data.items())[:3])
                return json.dumps(truncated_data, indent=2, cls=NumpyEncoder)
        except:
            # Si no es JSON, truncar por líneas
            lines = prompt.split('\n')
            if len(lines) > 20:  # Reducido a 20 líneas
                return '\n'.join(lines[:20]) + "\n... (truncated)"
        return prompt[:500]  # Reducido a 500 caracteres

    def _analyze_sensor_data(self, data: pd.DataFrame) -> str:
        """Analiza los datos de sensores de manera más eficiente."""
        try:
            sensor_summary = preprocess_data(data)
            
            # Convertir todos los valores numéricos a tipos nativos de Python
            def convert_numpy_types(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    return {k: convert_numpy_types(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numpy_types(item) for item in obj]
                return obj

            sensor_summary = convert_numpy_types(sensor_summary)
            cache_key = self._get_cache_key(sensor_summary)
            
            prompt = f"""Analiza los siguientes datos de sensores y proporciona un resumen conciso:

{json.dumps(sensor_summary, indent=2, cls=NumpyEncoder)}

Enfócate en:
1. Patrones principales en los datos
2. Valores atípicos significativos
3. Tendencias observadas
4. Recomendaciones clave

Responde de manera concisa y estructurada."""

            return self._make_api_request(prompt, cache_key)
        except Exception as e:
            logging.error(f"Error in sensor data analysis: {str(e)}", exc_info=True)
            return f"Error al analizar datos de sensores: {str(e)}"

    def _analyze_production_data(self, bb_op_data: pd.DataFrame) -> str:
        """Analiza los datos de producción de manera más eficiente."""
        try:
            production_summary = preprocess_data(bb_op_data)
            
            # Convertir todos los valores numéricos a tipos nativos de Python
            def convert_numpy_types(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    return {k: convert_numpy_types(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numpy_types(item) for item in obj]
                return obj

            production_summary = convert_numpy_types(production_summary)
            cache_key = self._get_cache_key(production_summary)
            
            prompt = f"""Analiza los siguientes datos de producción y proporciona un resumen conciso:

{json.dumps(production_summary, indent=2, cls=NumpyEncoder)}

Enfócate en:
1. Eficiencia de producción
2. Patrones de producción
3. Posibles mejoras
4. Recomendaciones clave

Responde de manera concisa y estructurada."""

            return self._make_api_request(prompt, cache_key)
        except Exception as e:
            logging.error(f"Error in production data analysis: {str(e)}", exc_info=True)
            return f"Error al analizar datos de producción: {str(e)}"

    def _analyze_correlations(self, data: pd.DataFrame, bb_op_data: pd.DataFrame) -> str:
        """Analiza las correlaciones entre sensores y producción."""
        try:
            # Preparar datos para análisis de correlación
            sensor_summary = preprocess_data(data)
            production_summary = preprocess_data(bb_op_data)
            
            # Convertir todos los valores numéricos a tipos nativos de Python
            def convert_numpy_types(obj):
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    return {k: convert_numpy_types(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_numpy_types(item) for item in obj]
                return obj

            correlation_data = {
                "sensor_data": convert_numpy_types(sensor_summary),
                "production_data": convert_numpy_types(production_summary)
            }
            
            cache_key = self._get_cache_key(correlation_data)
            
            prompt = f"""Analiza las correlaciones entre los datos de sensores y producción:

{json.dumps(correlation_data, indent=2, cls=NumpyEncoder)}

Enfócate en:
1. Correlaciones significativas
2. Impacto en la producción
3. Oportunidades de optimización
4. Recomendaciones clave

Responde de manera concisa y estructurada."""

            return self._make_api_request(prompt, cache_key)
        except Exception as e:
            logging.error(f"Error in correlation analysis: {str(e)}", exc_info=True)
            return f"Error al analizar correlaciones: {str(e)}"

    def analyze_all_data(self, data: pd.DataFrame, bb_op_data: pd.DataFrame, 
                        bb_eve_data: pd.DataFrame, bb_alr_data: pd.DataFrame,
                        custom_prompt: str = None) -> str:
        """Analiza todos los conjuntos de datos disponibles."""
        try:
            # Preprocesar los datos
            sensor_data = preprocess_data(data)
            production_data = preprocess_data(bb_op_data) if not bb_op_data.empty else {"summary": "No hay datos de producción disponibles."}
            
            # Crear el prompt para el análisis
            if custom_prompt:
                prompt = custom_prompt
            else:
                prompt = """Analiza los siguientes datos y proporciona un resumen ejecutivo conciso:

Datos de Sensores:
{}

Datos de Producción:
{}

Enfócate en:
1. Patrones principales en los datos de sensores
2. Eficiencia y tendencias en producción
3. Correlaciones significativas entre sensores y producción
4. Recomendaciones clave para optimización

Proporciona un resumen conciso y estructurado.""".format(
                    json.dumps(sensor_data, indent=2, cls=NumpyEncoder),
                    json.dumps(production_data, indent=2, cls=NumpyEncoder)
                )
            
            # Realizar la solicitud a la API
            cache_key = self._get_cache_key({"sensor_data": sensor_data, "production_data": production_data})
            result = self._make_api_request(prompt, cache_key)
            
            return result
            
        except Exception as e:
            logging.error(f"Error en analyze_all_data: {str(e)}", exc_info=True)
            return f"Error al analizar datos: {str(e)}"
    
    def analyze_specific_data(self, data_type: str, data: pd.DataFrame) -> str:
        """
        Analiza un conjunto específico de datos.
        
        Args:
            data_type: Tipo de datos ('main', 'bb_op', 'bb_eve', 'bb_alr')
            data: DataFrame con los datos a analizar
            
        Returns:
            str: Análisis del conjunto de datos específico
        """
        start_time = time.time()
        logging.debug(f"Starting analyze_specific_data for {data_type}")
        
        if data.empty:
            logging.warning(f"No data available for {data_type}")
            return f"No hay datos disponibles para {data_type}"
        
        # Preparar resumen de datos
        data_prep_start = time.time()
        data_summary = {
            "total_records": len(data),
            "columns": data.columns.tolist(),
            "date_range": [str(data['Date [dd/mm/yyyy]'].min()), str(data['Date [dd/mm/yyyy]'].max())] if 'Date [dd/mm/yyyy]' in data.columns else []
        }
        
        if 'Event type' in data.columns:
            data_summary["event_types"] = data['Event type'].unique().tolist()
        
        logging.debug(f"Data preparation took {time.time() - data_prep_start:.2f} seconds")
        
        prompt = f"""Analiza los datos de {data_type}:
        {json.dumps(data_summary, indent=2)}

        Enfócate en:
        1. Patrones y anomalías
        2. Posibles problemas
        3. Recomendaciones de mejora
        """
        
        try:
            api_start = time.time()
            response = self._make_api_request(prompt)
            logging.debug(f"API request took {time.time() - api_start:.2f} seconds")
            logging.debug(f"Total analyze_specific_data execution took {time.time() - start_time:.2f} seconds")
            return response
        except Exception as e:
            logging.error(f"Error in analyze_specific_data: {str(e)}", exc_info=True)
            return f"Error al analizar datos: {str(e)}"
    
    def analyze_correlations(self, data: pd.DataFrame, bb_op_data: pd.DataFrame, 
                           bb_eve_data: pd.DataFrame, bb_alr_data: pd.DataFrame) -> str:
        """
        Analiza correlaciones entre los diferentes conjuntos de datos.
        
        Args:
            data: DataFrame principal
            bb_op_data: DataFrame de operación
            bb_eve_data: DataFrame de eventos
            bb_alr_data: DataFrame de alarmas
            
        Returns:
            str: Análisis de correlaciones
        """
        start_time = time.time()
        logging.debug("Starting analyze_correlations")
        
        # Preparar resumen de correlaciones
        data_prep_start = time.time()
        correlations = {
            "main_to_op": "Disponible" if not data.empty and not bb_op_data.empty else "No disponible",
            "main_to_eve": "Disponible" if not data.empty and not bb_eve_data.empty else "No disponible",
            "main_to_alr": "Disponible" if not data.empty and not bb_alr_data.empty else "No disponible",
            "op_to_eve": "Disponible" if not bb_op_data.empty and not bb_eve_data.empty else "No disponible",
            "op_to_alr": "Disponible" if not bb_op_data.empty and not bb_alr_data.empty else "No disponible",
            "eve_to_alr": "Disponible" if not bb_eve_data.empty and not bb_alr_data.empty else "No disponible"
        }
        logging.debug(f"Data preparation took {time.time() - data_prep_start:.2f} seconds")
        
        prompt = f"""Analiza las correlaciones disponibles:
        {json.dumps(correlations, indent=2)}

        Enfócate en:
        1. Patrones entre conjuntos
        2. Relaciones causales
        3. Recomendaciones basadas en correlaciones
        """
        
        try:
            api_start = time.time()
            response = self._make_api_request(prompt)
            logging.debug(f"API request took {time.time() - api_start:.2f} seconds")
            logging.debug(f"Total analyze_correlations execution took {time.time() - start_time:.2f} seconds")
            return response
        except Exception as e:
            logging.error(f"Error in analyze_correlations: {str(e)}", exc_info=True)
            return f"Error al analizar datos: {str(e)}"
    
    def analyze_production_data(self, data: Dict, context: str) -> str:
        """
        Analiza datos de producción usando el modelo local.
        
        Args:
            data (Dict): Datos de producción a analizar
            context (str): Contexto adicional para el análisis
            
        Returns:
            str: Análisis generado por el modelo
        """
        prompt = f"""
        Eres un experto en análisis de datos de producción industrial. 
        Usa la siguiente documentación técnica como referencia:

        {self.machine_docs}

        Analiza los siguientes datos de producción:
        {json.dumps(data, indent=2)}
        
        Contexto: {context}
        
        Proporciona un análisis detallado y recomendaciones basadas en la documentación técnica.
        Incluye:
        1. Análisis de patrones
        2. Posibles problemas o anomalías
        3. Recomendaciones de optimización
        4. Sugerencias de mantenimiento
        """
        
        try:
            response = self._make_api_request(prompt)
            return response
        except Exception as e:
            return f"Error al analizar datos: {str(e)}"
    
    def get_recommendations(self, event_type: str, parameters: Dict) -> str:
        """
        Obtiene recomendaciones basadas en el tipo de evento y parámetros.
        
        Args:
            event_type (str): Tipo de evento a analizar
            parameters (Dict): Parámetros del evento
            
        Returns:
            str: Recomendaciones generadas por el modelo
        """
        # Preparar el contexto con la documentación técnica
        machine_info = self.machine_docs.get("machine_info", {})
        common_issues = self.machine_docs.get("common_issues", {})
        diagnostic_procedures = self.machine_docs.get("diagnostic_procedures", {})
        
        prompt = f"""
        Eres un experto en análisis de datos de producción industrial. 
        Usa la siguiente documentación técnica como referencia:

        Información de la máquina:
        {json.dumps(machine_info, indent=2)}

        Problemas comunes y soluciones:
        {json.dumps(common_issues, indent=2)}

        Procedimientos de diagnóstico:
        {json.dumps(diagnostic_procedures, indent=2)}

        Basado en la documentación técnica, analiza el siguiente evento:
        Tipo: {event_type}
        Parámetros: {json.dumps(parameters, indent=2)}
        
        Proporciona un análisis estructurado que incluya:
        1. Análisis del evento:
           - Descripción detallada del problema
           - Componentes afectados
           - Impacto en la producción

        2. Posibles causas:
           - Causas más probables basadas en la documentación
           - Factores contribuyentes
           - Condiciones que podrían agravar el problema

        3. Recomendaciones de solución:
           - Pasos específicos a seguir
           - Procedimientos de diagnóstico recomendados
           - Soluciones técnicas basadas en la documentación

        4. Medidas preventivas:
           - Mantenimiento recomendado
           - Monitoreo a implementar
           - Mejoras en procedimientos

        Responde de manera clara y estructurada, enfocándote en soluciones prácticas y accionables.
        """

        print("\n========== PROMPT ENVIADO AL MODELO ==========")
        print(prompt)
        print("========== FIN DEL PROMPT ==========")
        logging.debug(f"PROMPT ENVIADO AL MODELO:\n{prompt}")

        try:
            response = self._make_api_request(prompt)
            return response
        except Exception as e:
            logging.error(f"Error al obtener recomendaciones: {str(e)}")
            return f"Error al obtener recomendaciones: {str(e)}"
    
    def analyze_event_patterns(self, patterns: List[Dict]) -> str:
        """
        Analiza patrones de eventos.
        
        Args:
            patterns (List[Dict]): Lista de patrones a analizar
            
        Returns:
            str: Análisis de patrones
        """
        prompt = f"""
        Eres un experto en análisis de datos de producción industrial. 
        Usa la siguiente documentación técnica como referencia:

        {self.machine_docs}

        Analiza los siguientes patrones de eventos:
        {json.dumps(patterns, indent=2)}
        
        Proporciona:
        1. Identificación de patrones anómalos
        2. Posibles causas
        3. Recomendaciones de optimización
        4. Sugerencias de mantenimiento preventivo
        """
        
        try:
            response = self._make_api_request(prompt)
            return response
        except Exception as e:
            return f"Error al analizar patrones: {str(e)}"
    
    def analyze_sensors_and_production(self, data: pd.DataFrame, bb_op_data: pd.DataFrame) -> str:
        """
        Analiza específicamente la relación entre datos de sensores y producción.
        
        Args:
            data: DataFrame con datos de sensores (BBProdMon_)
            bb_op_data: DataFrame con datos de producción (BBOp_)
        """
        start_time = time.time()
        logging.debug("Starting analyze_sensors_and_production")
        
        if data.empty or bb_op_data.empty:
            logging.warning("Missing data for sensors and production analysis")
            return "Se requieren datos de sensores y producción para el análisis."

        # Ordenar datos por fecha
        data_prep_start = time.time()
        data = data.sort_values('Date [dd/mm/yyyy]', ascending=False)
        bb_op_data = bb_op_data.sort_values('Ora start', ascending=False)

        # Preparar resumen enfocado en sensores y producción
        analysis_summary = {
            "sensor_analysis": {
                "total_records": len(data),
                "date_range": [str(data['Date [dd/mm/yyyy]'].min()), str(data['Date [dd/mm/yyyy]'].max())],
                "sensors": data.columns.tolist(),
                # Reducir a las últimas 8 lecturas en lugar de 15
                "recent_readings": data.head(8).astype(str).to_dict('records')
            },
            "production_analysis": {
                "total_records": len(bb_op_data),
                "date_range": [str(bb_op_data['Ora start'].min()), str(bb_op_data['Ora start'].max())],
                # Reducir a las últimas 5 producciones en lugar de 8
                "recent_productions": bb_op_data.head(5).astype(str).to_dict('records'),
                "production_metrics": {
                    "total_volume": round(float(bb_op_data['Concrete_Cons'].sum()), 1) if 'Concrete_Cons' in bb_op_data.columns else 0,
                    "avg_production_rate": round(float(bb_op_data['Production_Rate'].mean()), 1) if 'Production_Rate' in bb_op_data.columns else 0
                }
            }
        }
        logging.debug(f"Data preparation took {time.time() - data_prep_start:.2f} seconds")

        prompt = f"""Analiza la relación entre sensores y producción:

        Sensores:
        {json.dumps(analysis_summary['sensor_analysis'], indent=2)}

        Producción:
        {json.dumps(analysis_summary['production_analysis'], indent=2)}

        Enfócate en:
        1. Patrones en sensores durante producción
        2. Eficiencia basada en lecturas
        3. Optimizaciones posibles
        4. Recomendaciones de mantenimiento
        """

        try:
            api_start = time.time()
            response = self._make_api_request(prompt)
            logging.debug(f"API request took {time.time() - api_start:.2f} seconds")
            logging.debug(f"Total analyze_sensors_and_production execution took {time.time() - start_time:.2f} seconds")
            return response
        except Exception as e:
            logging.error(f"Error in analyze_sensors_and_production: {str(e)}", exc_info=True)
            return f"Error al analizar datos: {str(e)}"

    def _get_available_model(self, preferred_model):
        """Verifica los modelos disponibles en Ollama y selecciona el más adecuado."""
        try:
            resp = requests.get(f"{self.base_url}/api/tags", timeout=10)
            resp.raise_for_status()
            tags = resp.json().get('models', [])
            model_names = [m['name'] for m in tags]
            # Coincidencia exacta
            if preferred_model in model_names:
                return preferred_model
            # Coincidencia con ':latest'
            if preferred_model+':latest' in model_names:
                logging.warning(f"Modelo '{preferred_model}' no encontrado, usando '{preferred_model}:latest'")
                return preferred_model+':latest'
            # Buscar modelo que contenga el nombre base
            for m in model_names:
                if preferred_model in m:
                    logging.warning(f"Modelo '{preferred_model}' no encontrado, usando '{m}'")
                    return m
            # Usar el primero disponible
            if model_names:
                logging.warning(f"Modelo '{preferred_model}' no encontrado, usando '{model_names[0]}'")
                return model_names[0]
            logging.error("No hay modelos disponibles en Ollama.")
            raise RuntimeError("No hay modelos disponibles en Ollama.")
        except Exception as e:
            logging.error(f"Error al obtener modelos de Ollama: {e}")
            return preferred_model  # Intentar con el preferido aunque falle la consulta 