from flask import Flask, render_template, request, jsonify, session
from openai_analyzer import OpenAIAnalyzer
import os
import json
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Para manejar sesiones 

analyzer = OpenAIAnalyzer(model="gpt-3.5-turbo")

CONVERSATIONS_FILE = 'conversaciones.json'

# Utilidades para historial

def load_conversations():
    if not os.path.exists(CONVERSATIONS_FILE):
        return []
    with open(CONVERSATIONS_FILE, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except Exception:
            return []

def save_conversation_record(sn, cliente, conversation_id):
    conversations = load_conversations()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conversations.append({
        'sn': sn,
        'cliente': cliente,
        'fecha_hora': now,
        'conversation_id': conversation_id
    })
    with open(CONVERSATIONS_FILE, 'w', encoding='utf-8') as f:
        json.dump(conversations, f, indent=2, ensure_ascii=False)

def search_conversations(query):
    conversations = load_conversations()
    query = query.lower()
    results = []
    for conv in conversations:
        if (query in conv['sn'].lower() or
            query in conv['cliente'].lower() or
            query in conv['fecha_hora'].lower()):
            results.append(conv)
    return results

# Estructura para almacenar las conversaciones en memoria
conversations = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/conversation/new', methods=['POST'])
def new_conversation():
    data = request.json
    sn = data.get('sn', '').strip()
    cliente = data.get('cliente', '').strip()
    if not sn or not cliente:
        return jsonify({'error': 'Debe proporcionar S/N y cliente'}), 400
    conversation_id = f"CONV_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{sn}"
    conversations[conversation_id] = {
        'messages': [],
        'title': f'Conversación {sn} - {cliente}',
        'sn': sn,
        'cliente': cliente,
        'fecha_hora': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    save_conversation_record(sn, cliente, conversation_id)
    return jsonify({'conversation_id': conversation_id})

@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.json
    message = data.get('message')
    conversation_id = data.get('conversation_id')
    if not conversation_id or conversation_id not in conversations:
        return jsonify({'error': 'Conversación no encontrada'}), 404
    # Agregar mensaje del usuario
    conversations[conversation_id]['messages'].append({
        'role': 'user',
        'content': message,
        'timestamp': datetime.now().isoformat()
    })
    # Obtener respuesta del modelo
    try:
        response = analyzer.get_recommendations('troubleshooting', {
            'message': message,
            'conversation_history': conversations[conversation_id]['messages']
        })
        conversations[conversation_id]['messages'].append({
            'role': 'assistant',
            'content': response,
            'timestamp': datetime.now().isoformat()
        })
        return jsonify({
            'conversation_id': conversation_id,
            'response': response
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/conversations', methods=['GET'])
def get_conversations():
    # Devuelve el historial guardado en el archivo
    return jsonify(load_conversations())

@app.route('/api/conversations/search', methods=['GET'])
def search_conversations_endpoint():
    query = request.args.get('q', '').strip()
    if not query:
        return jsonify([])
    results = search_conversations(query)
    return jsonify(results)

@app.route('/api/conversations/<conversation_id>', methods=['GET'])
def get_conversation(conversation_id):
    if conversation_id in conversations:
        return jsonify(conversations[conversation_id])
    return jsonify({'error': 'Conversación no encontrada'}), 404

@app.route('/api/conversations/<conversation_id>/title', methods=['PUT'])
def update_conversation_title(conversation_id):
    if conversation_id in conversations:
        data = request.json
        conversations[conversation_id]['title'] = data.get('title', 'Nueva Conversación')
        return jsonify({'success': True})
    return jsonify({'error': 'Conversación no encontrada'}), 404

if __name__ == '__main__':
    app.run(debug=True) 