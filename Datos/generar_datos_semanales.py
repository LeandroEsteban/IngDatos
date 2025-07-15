import random
from datetime import datetime, timedelta
import mysql.connector


def main():
    # Configuración temporal - DESDE ENERO
    fecha_inicio = datetime(2025, 1, 6)  # Primer lunes de enero 2025
    fecha_actual = datetime.now()

    # Generar fechas semanales
    fechas_semanales = []
    fecha_iter = fecha_inicio
    while fecha_iter <= fecha_actual:
        fechas_semanales.append(fecha_iter)
        fecha_iter += timedelta(days=7)

    print(f"📅 Generando {len(fechas_semanales)} semanas de datos")

    try:
        # Conectar a MySQL
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='l3o.%data546.',
            database='starbucks_gold'
        )
        cursor = conn.cursor()

        print("🔄 Limpiando tablas existentes...")
        # Limpiar tablas existentes
        cursor.execute("DELETE FROM ventas_edad")
        cursor.execute("DELETE FROM ventas_categoria")
        cursor.execute("DELETE FROM ventas_jornada")

        # 1. INSERTAR DATOS DE GRUPOS ETARIOS
        print("📊 Insertando datos de grupos etarios...")
        grupos_etarios = ['18-25', '26-35', '36-50', '51+']
        patrones_edad = {
            '18-25': {'base': 11, 'variacion': 3},
            '26-35': {'base': 32, 'variacion': 4},
            '36-50': {'base': 57, 'variacion': 5},
            '51+': {'base': 12, 'variacion': 2}
        }

        id_edad = 1
        for fecha in fechas_semanales:
            porcentajes_semana = {}

            # Generar porcentajes para cada grupo
            for grupo in grupos_etarios:
                patron = patrones_edad[grupo]
                valor = patron['base'] + random.uniform(-patron['variacion'], patron['variacion'])
                porcentajes_semana[grupo] = max(5, valor)

            # Normalizar para que sumen 100%
            total = sum(porcentajes_semana.values())
            factor = 100 / total

            # Insertar cada grupo
            for grupo in grupos_etarios:
                porcentaje_final = round(porcentajes_semana[grupo] * factor, 2)
                query = """
                        INSERT INTO ventas_edad (id_ventas_edad, fecha_registro, grupo_etario, porcentaje)
                        VALUES (%s, %s, %s, %s) \
                        """
                cursor.execute(query, (id_edad, fecha.strftime('%Y-%m-%d'), grupo, porcentaje_final))
                id_edad += 1

        print(f"✅ Insertados {id_edad - 1} registros en ventas_edad")

        # 2. INSERTAR DATOS DE CATEGORÍAS
        print("📊 Insertando datos de categorías...")
        categorias = ['Bebidas Calientes', 'Bebidas Frías', 'Comida', 'Accesorios']
        patrones_categoria = {
            'Bebidas Calientes': {'base': 45, 'variacion': 8, 'cantidad_base': 400},
            'Bebidas Frías': {'base': 35, 'variacion': 6, 'cantidad_base': 320},
            'Comida': {'base': 15, 'variacion': 3, 'cantidad_base': 150},
            'Accesorios': {'base': 5, 'variacion': 2, 'cantidad_base': 50}
        }

        id_categoria = 1
        for fecha in fechas_semanales:
            porcentajes_semana = {}
            cantidades_semana = {}
            mes = fecha.month

            for categoria in categorias:
                patron = patrones_categoria[categoria]
                valor_porcentaje = patron['base']
                valor_cantidad = patron['cantidad_base']

                # Estacionalidad
                if categoria == 'Bebidas Calientes' and mes in [6, 7, 8]:  # Invierno
                    valor_porcentaje += 8
                    valor_cantidad += 80
                elif categoria == 'Bebidas Frías' and mes in [12, 1, 2]:  # Verano
                    valor_porcentaje += 8
                    valor_cantidad += 100

                valor_porcentaje += random.uniform(-patron['variacion'], patron['variacion'])
                valor_cantidad += random.uniform(-50, 50)

                porcentajes_semana[categoria] = max(2, valor_porcentaje)
                cantidades_semana[categoria] = max(20, int(valor_cantidad))

            # Normalizar porcentajes
            total = sum(porcentajes_semana.values())
            factor = 100 / total

            for categoria in categorias:
                porcentaje_final = round(porcentajes_semana[categoria] * factor, 2)
                cantidad_final = cantidades_semana[categoria]

                query = """
                        INSERT INTO ventas_categoria (id_ventas_categoria, fecha_registro, categoria, cantidad, porcentaje)
                        VALUES (%s, %s, %s, %s, %s) \
                        """
                cursor.execute(query,
                               (id_categoria, fecha.strftime('%Y-%m-%d'), categoria, cantidad_final, porcentaje_final))
                id_categoria += 1

        print(f"✅ Insertados {id_categoria - 1} registros en ventas_categoria")

        # 3. INSERTAR DATOS DE JORNADAS
        print("📊 Insertando datos de jornadas...")
        patrones_jornada = {
            'mañana': {'base': 380, 'variacion': 60},
            'tarde': {'base': 320, 'variacion': 50},
            'noche': {'base': 180, 'variacion': 40}
        }

        id_jornada = 1
        for fecha in fechas_semanales:
            # Factor de temporada
            mes = fecha.month
            if mes in [12, 1, 2]:  # Verano - más ventas
                factor_temporada = 1.15
            elif mes in [6, 7, 8]:  # Invierno - ventas normales
                factor_temporada = 1.0
            else:  # Transiciones
                factor_temporada = 1.05

            # Calcular cantidades para cada jornada
            cantidad_mañana = patrones_jornada['mañana']['base'] * factor_temporada
            cantidad_mañana += random.uniform(-patrones_jornada['mañana']['variacion'],
                                              patrones_jornada['mañana']['variacion'])
            cantidad_mañana = max(80, int(cantidad_mañana))

            cantidad_tarde = patrones_jornada['tarde']['base'] * factor_temporada
            cantidad_tarde += random.uniform(-patrones_jornada['tarde']['variacion'],
                                             patrones_jornada['tarde']['variacion'])
            cantidad_tarde = max(80, int(cantidad_tarde))

            cantidad_noche = patrones_jornada['noche']['base'] * factor_temporada
            cantidad_noche += random.uniform(-patrones_jornada['noche']['variacion'],
                                             patrones_jornada['noche']['variacion'])
            cantidad_noche = max(50, int(cantidad_noche))

            # Determinar período más activo
            ventas_totales = {'Mañana': cantidad_mañana, 'Tarde': cantidad_tarde, 'Noche': cantidad_noche}
            periodo_mas_activo = max(ventas_totales, key=ventas_totales.get)

            query = """
                    INSERT INTO ventas_jornada (id_ventas_jornada, fecha_registro, cantidad_ventas_mañana, \
                                                cantidad_ventas_tarde, cantidad_ventas_noche, periodo_mas_activo)
                    VALUES (%s, %s, %s, %s, %s, %s) \
                    """
            cursor.execute(query,
                           (id_jornada, fecha.strftime('%Y-%m-%d'), cantidad_mañana, cantidad_tarde, cantidad_noche,
                            periodo_mas_activo))
            id_jornada += 1

        print(f"✅ Insertados {id_jornada - 1} registros en ventas_jornada")

        # Confirmar cambios
        conn.commit()

        # Mostrar resumen
        print("\n📊 RESUMEN DE DATOS INSERTADOS:")

        cursor.execute("SELECT COUNT(*) FROM ventas_edad")
        count_edad = cursor.fetchone()[0]
        print(f"   🎯 ventas_edad: {count_edad} registros")

        cursor.execute("SELECT COUNT(*) FROM ventas_categoria")
        count_categoria = cursor.fetchone()[0]
        print(f"   🎯 ventas_categoria: {count_categoria} registros")

        cursor.execute("SELECT COUNT(*) FROM ventas_jornada")
        count_jornada = cursor.fetchone()[0]
        print(f"   🎯 ventas_jornada: {count_jornada} registros")

        print(f"\n🎉 ¡Proceso completado exitosamente!")
        print(f"📈 Se generaron datos para {len(fechas_semanales)} semanas")
        print(f"📅 Desde: {fecha_inicio.strftime('%Y-%m-%d')}")
        print(f"📅 Hasta: {fecha_actual.strftime('%Y-%m-%d')}")

    except mysql.connector.Error as err:
        print(f"❌ Error en MySQL: {err}")
    except Exception as e:
        print(f"❌ Error general: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("🔌 Conexión a MySQL cerrada")


if __name__ == "__main__":
    main()