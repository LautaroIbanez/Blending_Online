from flask import Flask, render_template, request, jsonify, send_from_directory, send_file
from openai_analyzer import OpenAIAnalyzer
import os
import json
from datetime import datetime
import pandas as pd
import io
import numpy as np
from dictionary import header_translation, tag_header_translation
import uuid

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Inicializar el analizador de OpenAI
try:
    analyzer = OpenAIAnalyzer(
        api_key = "API_KEY", 
        model="gpt-3.5-turbo"
    )
except Exception as e:
    print(f"Error al inicializar OpenAI: {str(e)}")
    analyzer = None

CONVERSATIONS_FILE = 'conversations_history.json'
conversations = []

# Utilidades para conversaciones

def load_conversations():
    if not os.path.exists(CONVERSATIONS_FILE):
        return []
    with open(CONVERSATIONS_FILE, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except Exception:
            return []

def save_conversations(data):
    with open(CONVERSATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# Cargar conversaciones al iniciar
conversations = load_conversations()

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')

@app.route('/api/conversations', methods=['GET'])
def list_conversations():
    # Solo metadatos
    return jsonify([
        {
            'id': c['id'],
            'sn': c['sn'],
            'cliente': c['cliente'],
            'fecha': c['fecha']
        } for c in conversations
    ])

@app.route('/api/conversation/new', methods=['POST'])
def new_conversation():
    data = request.json
    sn = data.get('sn', '').strip()
    cliente = data.get('cliente', '').strip()
    if not sn or not cliente:
        return jsonify({'error': 'Debe proporcionar S/N y cliente'}), 400
    conv_id = str(uuid.uuid4())
    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conv = {
        'id': conv_id,
        'sn': sn,
        'cliente': cliente,
        'fecha': fecha,
        'messages': []
    }
    conversations.append(conv)
    save_conversations(conversations)
    return jsonify({'id': conv_id, 'sn': sn, 'cliente': cliente, 'fecha': fecha})

@app.route('/api/conversation/<conv_id>', methods=['GET'])
def get_conversation(conv_id):
    conv = next((c for c in conversations if c['id'] == conv_id), None)
    if not conv:
        return jsonify({'error': 'Conversación no encontrada'}), 404
    return jsonify(conv)

@app.route('/api/conversation/<conv_id>', methods=['DELETE'])
def delete_conversation(conv_id):
    global conversations
    idx = next((i for i, c in enumerate(conversations) if c['id'] == conv_id), None)
    if idx is None:
        return jsonify({'error': 'Conversación no encontrada'}), 404
    conversations.pop(idx)
    save_conversations(conversations)
    return jsonify({'success': True})

@app.route('/api/conversation/<conv_id>/chat', methods=['POST'])
def chat(conv_id):
    conv = next((c for c in conversations if c['id'] == conv_id), None)
    if not conv:
        return jsonify({'error': 'Conversación no encontrada'}), 404
    message = request.form.get('message', '').strip()
    files = request.files.getlist('archivos')
    timestamp = datetime.now().isoformat()

    if not message and not files:
        return jsonify({'error': 'Debe ingresar un mensaje o adjuntar archivos'}), 400

    # Guardar mensaje del usuario
    user_msg = {
        'role': 'user',
        'content': message + (f"\nArchivos adjuntos: {', '.join([f.filename for f in files])}" if files else ''),
        'timestamp': timestamp
    }
    conv['messages'].append(user_msg)
    save_conversations(conversations)

    # Procesar archivos si existen (puedes expandir esto según tus necesidades)
    analysis_results = []
    if files:
        for file in files:
            if file.filename.startswith('BBOp_'):
                try:
                    df = pd.read_csv(file)
                    analysis_results.append(f"Análisis de {file.filename}:\n{process_bbop_file(df)}")
                except Exception as e:
                    analysis_results.append(f"Error al procesar {file.filename}: {str(e)}")
            elif file.filename.startswith('Events_'):
                try:
                    df = pd.read_csv(file)
                    analysis_results.append(f"Análisis de {file.filename}:\n{process_events_file(df)}")
                except Exception as e:
                    analysis_results.append(f"Error al procesar {file.filename}: {str(e)}")

    # Preparar mensaje para la IA
    analysis_text = "\n".join(analysis_results) if analysis_results else ""
    full_message = f"{message}\n\n{analysis_text}" if analysis_text else message

    # Obtener contexto (puedes personalizar esto)
    context = get_general_context()

    # Llamar a la IA
    try:
        response = analyzer.get_recommendations(
            conversation_history=conv['messages'],
            machine_docs=context,
            extra_messages=[{
                "role": "system",
                "content": (
                    "Eres un experto en soporte técnico de plantas industriales. "
                    "A partir del problema que describe el usuario, genera un troubleshooting como una lista de pasos principales, usando el formato 'Paso 1:', 'Paso 2:', 'Paso 3:', etc. en vez de numeración tipo '1.', '2.', etc. "
                    "Nunca uses subíndices, sublistas ni listas anidadas bajo ningún concepto. Cada paso debe ser autocontenido: acción clara (con icono 🛠️ si aplica), lógica de decisión Si/No (pero sin poner la pregunta '¿Funcionó? Sí/No' en el título), e indicación: 'Si: ...', 'No: ...', y explicación de causa/efecto dentro del mismo paso. "
                    "Cuando la respuesta sea negativa, explica brevemente la causa probable y el efecto, y qué hacer antes de volver a intentar, todo dentro del mismo paso. "
                    "No repitas el número de paso. No pongas preguntas fuera de la lista. Usa solo una lista principal. No uses subíndices, sublistas ni listas anidadas. "
                    "Usa iconos como 🛠️ para acciones y ⚠️ para advertencias. "
                    "Resalta los puntos clave en negrita y usa formato Markdown. "
                    "No incluyas explicaciones largas, pero sí pasos claros, detallados y con lógica Si/No explícita. "
                    "No incluyas una pregunta final para el usuario. Todos los pasos deben estar en un solo mensaje. "
                    "El troubleshooting debe finalizar siempre con un último paso que indique: 'Si el problema persiste, contactar al Servicio Técnico.' como última instancia. "
                    "Ejemplo de formato esperado:\n"
                    "Paso 1: 🛠️ Verifica la tensión de la correa.\n   - Si: ve al Paso 2.\n   - No: ajusta la correa y vuelve a probar. La causa probable es que la correa floja reduce la transmisión de potencia y afecta la presión.\n"
                    "Paso 2: 🛠️ Revisa el nivel de aceite.\n   - Si: ve al Paso 3.\n   - No: rellena el aceite y vuelve a probar. La causa probable es que el bajo nivel de aceite genera cavitación y pérdida de presión.\n"
                    "Paso 3: 🛠️ Comprueba el filtro.\n   - Si: el problema está resuelto.\n   - No: contactar al Servicio Técnico.\n"
                    "Recuerda: cada paso debe ser un solo ítem de la lista principal, sin subíndices ni sublistas. Si necesitas profundizar, hazlo dentro del mismo paso, nunca como subíndice."
                )
            }]
        )
    except Exception as e:
        return jsonify({'error': f'Error al consultar la IA: {str(e)}'}), 500

    # Guardar respuesta del asistente
    assistant_msg = {
        'role': 'assistant',
        'content': response,
        'timestamp': datetime.now().isoformat()
    }
    conv['messages'].append(assistant_msg)
    save_conversations(conversations)

    return jsonify({'response': response})

# Utilidades de contexto y análisis (puedes simplificar según tu flujo)
def get_general_context():
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        context_path = os.path.join(base_dir, 'models', 'machine_docs', 'General_Context.txt')
        if os.path.exists(context_path):
            with open(context_path, 'r', encoding='utf-8') as f:
                context = f.read().strip()
                if context:
                    return context
        return ''
    except Exception:
        return ''

def apply_header_translations(df):
    # Puedes dejar esto igual o simplificar
    for col in df.columns:
        if col in header_translation:
            df = df.rename(columns={col: header_translation[col]})
    return df

def process_bbop_file(df):
    try:
        df = apply_header_translations(df)
        last_3_rows = df.tail(3)
        summary = []
        for _, row in last_3_rows.iterrows():
            cycle_summary = []
            if 'Ora start' in row:
                cycle_summary.append(f"Inicio: {row['Ora start']}")
            if 'Ora stop' in row:
                cycle_summary.append(f"Fin: {row['Ora stop']}")
            if 'Nome ricetta' in row:
                cycle_summary.append(f"Receta: {row['Nome ricetta']}")
            if 'Produzione oraria [m³/h]' in row:
                cycle_summary.append(f"Tasa de producción: {row['Produzione oraria [m³/h]']} m³/h")
            if 'Calcestruzzo [m³]' in row:
                cycle_summary.append(f"Concreto producido: {row['Calcestruzzo [m³]']} m³")
            summary.append("\n".join(cycle_summary))
        return "\n\n".join(summary)
    except Exception as e:
        return f"Error al procesar archivo BBOp_: {str(e)}"

def process_events_file(df):
    try:
        df = apply_header_translations(df)
        last_10_events = df.tail(10)
        summary = []
        for _, row in last_10_events.iterrows():
            event_summary = []
            if 'Ora' in row:
                event_summary.append(f"Fecha/Hora: {row['Ora']}")
            if 'Evento' in row:
                event_summary.append(f"Evento: {row['Evento']}")
            if 'Descrizione' in row:
                event_summary.append(f"Descripción: {row['Descrizione']}")
            summary.append("\n".join(event_summary))
        return "\n\n".join(summary)
    except Exception as e:
        return f"Error al procesar archivo de eventos: {str(e)}"

@app.route('/logo.png')
def logo():
    return send_file('logo.png', mimetype='image/png')

if __name__ == '__main__':
    app.run(debug=True) 