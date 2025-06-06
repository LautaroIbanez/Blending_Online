import openai
import os
import logging

class OpenAIAnalyzer:
    def __init__(self, api_key=None, model="gpt-3.5-turbo"):
        """
        Inicializa el analizador de OpenAI.
        
        Args:
            api_key (str, optional): API key de OpenAI. Si no se proporciona, se buscará en la variable de entorno OPENAI_API_KEY.
            model (str, optional): Modelo a utilizar. Por defecto es "gpt-3.5-turbo".
            
        Raises:
            ValueError: Si no se proporciona API key ni está configurada en las variables de entorno.
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "No se encontró la API key de OpenAI. "
                "Por favor, configura la variable de entorno OPENAI_API_KEY o "
                "proporciona la API key al inicializar el analizador."
            )
        self.model = model
        logging.info(f"OpenAIAnalyzer inicializado con modelo: {model}")

    def get_recommendations(self, conversation_history, machine_docs=None, extra_messages=None):
        # Construir el contexto del sistema
        system_prompt = (
            "Eres un experto en soporte técnico de plantas industriales. "
            "Ayuda al usuario a resolver problemas técnicos con su máquina, "
            "usando la siguiente documentación técnica como referencia:\n"
        )
        if machine_docs:
            system_prompt += machine_docs[:2000]  # Limita a 2000 caracteres para no saturar el contexto

        # Construir el historial de mensajes para OpenAI
        messages = [{"role": "system", "content": system_prompt}]
        # Agregar mensajes extra (si existen)
        if extra_messages:
            messages.extend(extra_messages)
        # Agregar el historial de conversación (si existe)
        if conversation_history:
            for msg in conversation_history:
                messages.append({
                    "role": "user" if msg["role"] == "user" else "assistant",
                    "content": msg["content"]
                })

        # Logs de depuración
        logging.debug(f"[OpenAIAnalyzer] Usando API KEY: {'***' + self.api_key[-8:] if self.api_key else 'NO API KEY'}")
        logging.debug(f"[OpenAIAnalyzer] Modelo: {self.model}")
        logging.debug(f"[OpenAIAnalyzer] Mensajes enviados: {messages}")
        try:
            client = openai.OpenAI(api_key=self.api_key)
            logging.debug("[OpenAIAnalyzer] Enviando solicitud a OpenAI...")
            response = client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=0.3,
                max_tokens=2000
            )
            logging.debug("[OpenAIAnalyzer] Respuesta recibida de OpenAI")
            return response.choices[0].message.content.strip()
        except Exception as e:
            logging.error(f"[OpenAIAnalyzer] Error al llamar a OpenAI: {e}")
            raise 