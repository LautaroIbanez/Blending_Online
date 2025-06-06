from ollama_analyzer import OllamaAnalyzer

def test_ollama():
    try:
        # Inicializar el analizador
        analyzer = OllamaAnalyzer(model_name="mistral")
        
        # Crear un mensaje de prueba
        test_event_type = "Test Event"
        test_parameters = {
            "description": "Test message to verify Ollama connection",
            "timestamp": "2024-03-19 12:00:00"
        }
        
        # Obtener una respuesta
        response = analyzer.get_recommendations(
            event_type=test_event_type,
            parameters=test_parameters
        )
        
        print("Respuesta de Ollama:")
        print(response)
        print("\n¡La conexión con Ollama funciona correctamente!")
        
    except Exception as e:
        print(f"Error al conectar con Ollama: {str(e)}")

if __name__ == "__main__":
    test_ollama() 