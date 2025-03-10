import pandas as pd
from sqlalchemy import create_engine
from src.load import load

def test_load():
    """Test the load function."""
    # Crear una base de datos en memoria
    engine = create_engine("sqlite:///:memory:")

    # Datos de prueba
    data_frames = {
        "test_table": pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"]
        })
    }

    try:
        # Ejecutar la función load
        load(data_frames, engine)

        # Consultar la base de datos
        with engine.connect() as connection:
            result = connection.execute("SELECT * FROM test_table").fetchall()

        # Validar los resultados
        assert len(result) == 3
        assert result[0] == (1, "A")
        assert result[1] == (2, "B")
        assert result[2] == (3, "C")

        print("✅ ¡El test pasó exitosamente!")

    except Exception as e:
        print(f"❌ El test falló: {e}")
        raise
