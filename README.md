# Concrete Production Monitor

A web application for monitoring and analyzing concrete production data. This application allows users to upload and visualize data from various CSV files related to concrete production monitoring.

## Features

- Upload and process multiple CSV files (BBProdMon_, BBOp_, BBEve_, BBAlr_)
- Interactive data visualization with Plotly
- Filter data by date, hourly production, and event type
- Multi-language support
- Responsive web interface

## Local Development

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Run the application:
   ```bash
   python app.py
   ```
5. Open your browser and navigate to `http://localhost:8050`

## Deployment

This application is configured for deployment on Render. To deploy:

1. Create a new Web Service on Render
2. Connect your repository
3. Use the following settings:
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `gunicorn app:server`
   - Python Version: 3.9.0

## File Types

The application supports the following file types:

- BBProdMon_ or P_: Production monitoring data
- BBOp_ or E_: Operation data
- BBEve_: Event data
- BBAlr_: Alarm data

## Dependencies

- Dash
- Dash Bootstrap Components
- Pandas
- Plotly
- NumPy
- MySQL Connector
- Gunicorn

## Web de Soporte Técnico (Chatbot)

### Requisitos

- Python 3.8+
- Flask
- flask-cors
- ollama

Instala las dependencias:

```bash
pip install -r requirements.txt
```

### Ejecución

```bash
python app.py
```

La aplicación estará disponible en http://localhost:5000

### Estructura

- `app.py`: Servidor Flask y API para el chatbot.
- `templates/index.html`: Interfaz web del chat (debes crear este archivo).
- Las conversaciones se almacenan en memoria (puedes adaptar a base de datos si lo deseas).

### Personalización

- El modelo de IA utiliza la lógica de `ollama_analyzer.py` para responder preguntas técnicas sobre plantas de hormigón.
- Puedes modificar el prompt o la lógica de análisis en ese archivo. 