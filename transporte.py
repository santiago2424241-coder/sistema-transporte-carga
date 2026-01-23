"""
Sistema de Programación de Rutas y Cálculo de Costos para Tractomulas
Versión 3.4 - Conectado a Supabase (PostgreSQL) - CORREGIDO
Contexto: Colombia
Autor: Sistema de Gestión de Transporte de Carga
"""

import streamlit as st
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict
import io
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import pandas as pd
import locale
import plotly.express as px

# Configurar locale para formato colombiano
try:
    locale.setlocale(locale.LC_ALL, 'es_CO.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
    except:
        pass


# ==================== CONFIGURACIÓN SUPABASE ====================
SUPABASE_DB_URL = "postgresql://postgres.verwlkgitpllyneqxlao:Conejito800$@aws-0-us-west-2.pooler.supabase.com:6543/postgres?sslmode=require"


# ==================== FUNCIONES DE FORMATO ====================
def formatear_numero(valor):
    """Formatea un número al estilo colombiano: 5.000.000"""
    if valor is None:
        return "0"
    try:
        return f"{int(valor):,}".replace(',', '.')
    except:
        return str(valor)


def formatear_decimal(valor, decimales=2):
    """Formatea un número con decimales al estilo colombiano: 5.000.000,50"""
    if valor is None:
        return "0,00"
    try:
        formatted = f"{float(valor):,.{decimales}f}"
        # Reemplazar separadores: primero decimales, luego miles
        formatted = formatted.replace(',', 'TEMP')
        formatted = formatted.replace('.', ',')
        formatted = formatted.replace('TEMP', '.')
        return formatted
    except:
        return str(valor)


def limpiar_numero(texto):
    """Convierte texto con formato colombiano a número"""
    if not texto:
        return 0.0
    try:
        # Remover puntos de miles y reemplazar coma decimal por punto
        texto = str(texto).replace('.', '').replace(',', '.')
        return float(texto)
    except:
        return 0.0


# ==================== BASE DE DATOS SUPABASE (CORREGIDA V2) ====================
class DatabaseManager:
    """Gestor de base de datos Supabase (PostgreSQL) para trazabilidad"""

    def __init__(self):
        self.db_url = SUPABASE_DB_URL
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def init_database(self):
        """Crea las tablas si no existen (Sintaxis PostgreSQL)"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            # Tabla Viajes v4 (Versión actualizada)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS viajes_v4 (
                    id SERIAL PRIMARY KEY,
                    fecha_creacion TEXT NOT NULL,
                    placa TEXT NOT NULL,
                    conductor TEXT NOT NULL,
                    origen TEXT NOT NULL,
                    destino TEXT NOT NULL,
                    distancia_km REAL NOT NULL,
                    dias_viaje INTEGER NOT NULL,
                    es_frontera INTEGER NOT NULL,
                    hubo_parqueo INTEGER NOT NULL,
                    nomina_admin REAL,
                    nomina_conductor REAL,
                    comision_conductor REAL,
                    mantenimiento REAL,
                    seguros REAL,
                    tecnomecanica REAL,
                    llantas REAL,
                    aceite REAL,
                    combustible REAL,
                    galones_necesarios REAL,
                    flypass REAL,
                    peajes REAL,
                    cruce_frontera REAL,
                    hotel REAL,
                    comida REAL,
                    parqueo REAL,
                    cargue_descargue REAL,
                    otros REAL,
                    total_gastos REAL,
                    legalizacion REAL,
                    punto_equilibrio REAL,
                    valor_flete REAL,
                    utilidad REAL,
                    rentabilidad REAL,
                    anticipo REAL,
                    saldo REAL,
                    hubo_anticipo_empresa INTEGER,
                    ant_empresa REAL,
                    saldo_empresa REAL,
                    observaciones TEXT
                )
            ''')

            # Tabla de tractomulas
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tractomulas (
                    id SERIAL PRIMARY KEY,
                    placa TEXT UNIQUE NOT NULL,
                    consumo_km_galon REAL NOT NULL,
                    tipo TEXT NOT NULL
                )
            ''')

            # Tabla de conductores
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS conductores (
                    id SERIAL PRIMARY KEY,
                    nombre TEXT UNIQUE NOT NULL,
                    cedula TEXT NOT NULL
                )
            ''')

            # Tabla de rutas
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rutas (
                    id SERIAL PRIMARY KEY,
                    origen TEXT NOT NULL,
                    destino TEXT NOT NULL,
                    distancia_km REAL NOT NULL,
                    es_frontera INTEGER NOT NULL,
                    es_regional INTEGER NOT NULL,
                    es_aguachica INTEGER NOT NULL
                )
            ''')

            conn.commit()
            conn.close()
        except Exception as e:
            st.error(f"Error inicializando base de datos: {e}")

    def guardar_viaje(self, calculadora, observaciones=""):
        """Guarda un viaje en la base de datos de forma segura con HORA COLOMBIA"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            costos = calculadora.calcular_costos_totales()
            
            # --- CORRECCIÓN DE HORA ---
            # El servidor está en UTC, así que restamos 5 horas para tener hora Colombia
            hora_colombia = datetime.now() - timedelta(hours=5)
            fecha_actual = hora_colombia.strftime('%Y-%m-%d %H:%M:%S')
            # --------------------------

            # 1. Preparamos los datos en una tupla ordenada (39 elementos)
            datos_viaje = (
                fecha_actual,                                   # 1 (FECHA CORREGIDA)
                str(calculadora.tractomula.placa),              # 2
                str(calculadora.conductor.nombre),              # 3
                str(calculadora.ruta.origen),                   # 4
                str(calculadora.ruta.destino),                  # 5
                float(calculadora.ruta.distancia_km),           # 6
                int(calculadora.dias_viaje),                    # 7
                1 if calculadora.es_frontera else 0,            # 8
                1 if calculadora.hubo_parqueo else 0,           # 9
                costos['nomina_admin'],                         # 10
                costos['nomina_conductor'],                     # 11
                costos['comision_conductor'],                   # 12
                costos['mantenimiento'],                        # 13
                costos['seguros'],                              # 14
                costos['tecnomecanica'],                        # 15
                costos['llantas'],                              # 16
                costos['aceite'],                               # 17
                costos['combustible'],                          # 18
                costos['galones_necesarios'],                   # 19
                float(calculadora.flypass),                     # 20
                float(calculadora.peajes),                      # 21
                costos['cruce_frontera'],                       # 22
                float(calculadora.hotel),                       # 23
                float(calculadora.comida),                      # 24
                costos['parqueo'],                              # 25
                float(calculadora.cargue_descargue),            # 26
                float(calculadora.otros),                       # 27
                costos['total_gastos'],                         # 28
                costos['legalizacion'],                         # 29
                costos['punto_equilibrio'],                     # 30
                float(calculadora.valor_flete),                 # 31
                costos['utilidad'],                             # 32
                costos['rentabilidad'],                         # 33
                float(calculadora.anticipo),                    # 34
                costos['saldo'],                                # 35
                1 if calculadora.hubo_anticipo_empresa else 0,  # 36
                costos['ant_empresa'],                          # 37
                costos['saldo_empresa'],                        # 38
                str(observaciones)                              # 39
            )

            # 2. Ejecutamos la consulta
            sql_insert = '''
                INSERT INTO viajes_v4 (
                    fecha_creacion, placa, conductor, origen, destino, distancia_km,
                    dias_viaje, es_frontera, hubo_parqueo, nomina_admin, nomina_conductor,
                    comision_conductor, mantenimiento, seguros, tecnomecanica, llantas,
                    aceite, combustible, galones_necesarios, flypass, peajes,
                    cruce_frontera, hotel, comida, parqueo, cargue_descargue, otros,
                    total_gastos, legalizacion, punto_equilibrio, valor_flete,
                    utilidad, rentabilidad, anticipo, saldo, hubo_anticipo_empresa,
                    ant_empresa, saldo_empresa, observaciones
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                ) RETURNING id
            '''

            cursor.execute(sql_insert, datos_viaje)

            result = cursor.fetchone()
            if result:
                viaje_id = result[0]
            else:
                viaje_id = None
                st.warning("El viaje se guardó pero no se pudo recuperar el ID.")

            conn.commit()
            conn.close()
            return viaje_id

        except Exception as e:
            st.error(f"❌ Error detallado al guardar: {e}")
            return None

    def obtener_todos_viajes(self):
        """Obtiene todos los viajes ordenados por fecha"""
        conn = self.get_connection()
        query = "SELECT * FROM viajes_v4 ORDER BY fecha_creacion DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def buscar_viajes(self, fecha_inicio=None, fecha_fin=None, placa=None, conductor=None, origen=None, destino=None):
        """Busca viajes con filtros"""
        conn = self.get_connection()
        query = "SELECT * FROM viajes_v4 WHERE 1=1"
        params = []
        if fecha_inicio:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') <= %s"
            params.append(fecha_fin)
        if placa:
            query += " AND placa = %s"
            params.append(placa)
        if conductor:
            query += " AND conductor LIKE %s"
            params.append(f"%{conductor}%")
        if origen:
            query += " AND origen LIKE %s"
            params.append(f"%{origen}%")
        if destino:
            query += " AND destino LIKE %s"
            params.append(f"%{destino}%")
        query += " ORDER BY fecha_creacion DESC"
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df

    def obtener_viaje_por_id(self, viaje_id):
        """Obtiene un viaje específico por ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM viajes_v4 WHERE id = %s", (viaje_id,))
        viaje = cursor.fetchone()
        conn.close()
        return viaje

    def eliminar_viaje(self, viaje_id):
        """Elimina un viaje por ID"""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM viajes_v4 WHERE id = %s", (viaje_id,))
        conn.commit()
        conn.close()

    def obtener_estadisticas(self):
        """Obtiene estadísticas generales"""
        conn = self.get_connection()
        cursor = conn.cursor()
        stats = {}
        try:
            cursor.execute("SELECT COUNT(*) FROM viajes_v4")
            stats['total_viajes'] = cursor.fetchone()[0]
            cursor.execute("SELECT SUM(distancia_km) FROM viajes_v4")
            stats['total_km'] = cursor.fetchone()[0] or 0
            cursor.execute("SELECT SUM(total_gastos) FROM viajes_v4")
            stats['total_gastos'] = cursor.fetchone()[0] or 0
            cursor.execute("SELECT placa, COUNT(*) as total FROM viajes_v4 GROUP BY placa ORDER BY total DESC")
            stats['viajes_por_placa'] = cursor.fetchall()
            cursor.execute("SELECT conductor, COUNT(*) as total FROM viajes_v4 GROUP BY conductor ORDER BY total DESC")
            stats['viajes_por_conductor'] = cursor.fetchall()
            cursor.execute("SELECT origen, destino, COUNT(*) as total FROM viajes_v4 GROUP BY origen, destino ORDER BY total DESC LIMIT 5")
            stats['rutas_frecuentes'] = cursor.fetchall()
        except Exception:
            stats = {'total_viajes': 0, 'total_km': 0, 'total_gastos': 0, 'viajes_por_placa': [], 'viajes_por_conductor': [], 'rutas_frecuentes': []}
        conn.close()
        return stats

    def obtener_dashboard_data(self):
        """Obtiene datos para el dashboard"""
        conn = self.get_connection()
        cursor = conn.cursor()
        hoy = datetime.now()
        inicio_mes = hoy.replace(day=1).strftime('%Y-%m-%d')
        data = {}

        try:
            cursor.execute("""
                SELECT COUNT(*) as total_viajes, SUM(distancia_km) as total_km,
                       SUM(total_gastos) as total_gastos, SUM(valor_flete) as total_ingresos,
                       SUM(utilidad) as total_utilidad, AVG(utilidad) as utilidad_promedio
                FROM viajes_v4
                WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
            """, (inicio_mes,))
            row = cursor.fetchone()
            data['mes_actual'] = {
                'total_viajes': row[0] or 0,
                'total_km': row[1] or 0,
                'total_gastos': row[2] or 0,
                'total_ingresos': row[3] or 0,
                'total_utilidad': row[4] or 0,
                'utilidad_promedio': row[5] or 0
            }

            cursor.execute("""
                SELECT placa, COUNT(*) as viajes, SUM(total_gastos) as gastos,
                       SUM(valor_flete) as ingresos, SUM(utilidad) as utilidad
                FROM viajes_v4
                WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
                GROUP BY placa ORDER BY utilidad DESC
            """, (inicio_mes,))
            data['por_tractomula'] = cursor.fetchall()

            cursor.execute("""
                SELECT conductor, COUNT(*) as viajes, SUM(utilidad) as utilidad,
                       AVG(utilidad) as utilidad_promedio
                FROM viajes_v4
                WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
                GROUP BY conductor ORDER BY utilidad DESC
            """, (inicio_mes,))
            data['por_conductor'] = cursor.fetchall()

            cursor.execute("""
                SELECT origen, destino, COUNT(*) as viajes, AVG(utilidad) as utilidad_promedio,
                       SUM(utilidad) as utilidad_total
                FROM viajes_v4
                WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
                GROUP BY origen, destino ORDER BY utilidad_total DESC LIMIT 5
            """, (inicio_mes,))
            data['rutas_rentables'] = cursor.fetchall()

            cursor.execute("""
                SELECT to_char(to_date(fecha_creacion, 'YYYY-MM-DD'), 'YYYY-MM') as mes,
                       COUNT(*) as viajes, SUM(total_gastos) as gastos,
                       SUM(valor_flete) as ingresos, SUM(utilidad) as utilidad
                FROM viajes_v4
                WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '6 months'
                GROUP BY mes ORDER BY mes
            """)
            data['evolucion_6_meses'] = cursor.fetchall()

            cursor.execute("""
                SELECT fecha_creacion, placa, origen, destino, total_gastos, valor_flete, utilidad
                FROM viajes_v4 WHERE utilidad < 0 ORDER BY fecha_creacion DESC LIMIT 10
            """)
            data['viajes_no_rentables'] = cursor.fetchall()

            # Nuevas métricas para UT BRUTA, UT NETA, % UT
            cursor.execute("""
                SELECT SUM(valor_flete) as ut_bruta, SUM(utilidad) as ut_neta
                FROM viajes_v4 WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
            """, (inicio_mes,))
            row_ut = cursor.fetchone()
            ut_bruta = row_ut[0] or 0
            ut_neta = row_ut[1] or 0
            porcentaje_ut = (ut_neta / ut_bruta * 100) if ut_bruta > 0 else 0
            data['ut_bruta'] = ut_bruta
            data['ut_neta'] = ut_neta
            data['porcentaje_ut'] = porcentaje_ut
        
        except Exception:
             # En caso de error (tabla vacía o no existe), retornar valores en cero
            data = {k: 0 for k in ['ut_bruta', 'ut_neta', 'porcentaje_ut']}
            data['mes_actual'] = {k: 0 for k in ['total_viajes', 'total_km', 'total_gastos', 'total_ingresos', 'total_utilidad', 'utilidad_promedio']}
            data['por_tractomula'] = []
            data['por_conductor'] = []
            data['rutas_rentables'] = []
            data['evolucion_6_meses'] = []
            data['viajes_no_rentables'] = []

        conn.close()
        return data

    def obtener_totales_por_placa(self, fecha_inicio=None, fecha_fin=None):
        """Obtiene totales acumulados por placa con filtros de fecha"""
        conn = self.get_connection()
        query = """
            SELECT placa, SUM(valor_flete) as total_cxc, SUM(nomina_admin) as total_admin,
                   SUM(nomina_conductor) as total_parafiscales, SUM(comision_conductor) as total_comision,
                   SUM(mantenimiento) as total_mantenimiento, SUM(seguros) as total_seguros,
                   SUM(tecnomecanica) as total_tecnomecanica, SUM(llantas) as total_llantas,
                   SUM(aceite) as total_aceite, SUM(combustible) as total_combustible,
                   SUM(flypass) as total_flypass, SUM(peajes) as total_peajes,
                   SUM(cruce_frontera) as total_cruce_frontera, SUM(hotel) as total_hotel,
                   SUM(comida) as total_comida, SUM(parqueo) as total_parqueo,
                   SUM(cargue_descargue) as total_cargue_descargue, SUM(otros) as total_otros,
                   SUM(legalizacion) as total_legalizacion, SUM(anticipo) as total_anticipo,
                   SUM(saldo) as total_saldo, SUM(ant_empresa) as total_ant_empresa,
                   SUM(saldo_empresa) as total_saldo_empresa
            FROM viajes_v4 WHERE 1=1
        """
        params = []
        if fecha_inicio:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') <= %s"
            params.append(fecha_fin)
        query += " GROUP BY placa ORDER BY placa"

        try:
            df = pd.read_sql_query(query, conn, params=params)
            # Calcular compuestos
            if not df.empty:
                df['total_gastos'] = (
                    df['total_admin'] + df['total_parafiscales'] + df['total_comision'] +
                    df['total_mantenimiento'] + df['total_seguros'] + df['total_tecnomecanica'] +
                    df['total_llantas'] + df['total_aceite'] + df['total_combustible'] +
                    df['total_flypass'] + df['total_peajes'] + df['total_cruce_frontera'] +
                    df['total_hotel'] + df['total_comida'] + df['total_parqueo'] +
                    df['total_cargue_descargue'] + df['total_otros']
                )
                df['total_punto_equilibrio'] = df['total_gastos'] / 0.5
                df['total_ut'] = df['total_cxc'] - df['total_gastos']
                df['total_rentabilidad'] = (df['total_ut'] / df['total_cxc'] * 100).where(df['total_cxc'] != 0, 0)
        except Exception:
            df = pd.DataFrame()
            
        conn.close()
        return df

    # Métodos para tractomulas
    def guardar_tractomula(self, tractomula):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO tractomulas (placa, consumo_km_galon, tipo)
                VALUES (%s, %s, %s)
            ''', (tractomula.placa, tractomula.consumo_km_galon, tractomula.tipo))
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()

    def obtener_tractomulas(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tractomulas ORDER BY placa")
        tractomulas = []
        for row in cursor.fetchall():
            tractomulas.append(Tractomula(
                placa=row[1],
                consumo_km_galon=row[2],
                tipo=row[3]
            ))
        conn.close()
        return tractomulas

    def eliminar_tractomula(self, placa):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM tractomulas WHERE placa = %s", (placa,))
        conn.commit()
        conn.close()

    # Métodos para conductores
    def guardar_conductor(self, conductor):
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO conductores (nombre, cedula)
                VALUES (%s, %s)
            ''', (conductor.nombre, conductor.cedula))
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()

    def obtener_conductores(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM conductores ORDER BY nombre")
        conductores = []
        for row in cursor.fetchall():
            conductores.append(Conductor(nombre=row[1], cedula=row[2]))
        conn.close()
        return conductores

    def eliminar_conductor(self, nombre):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM conductores WHERE nombre = %s", (nombre,))
        conn.commit()
        conn.close()

    # Métodos para rutas
    # Métodos para rutas
    def guardar_ruta(self, ruta):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO rutas (origen, destino, distancia_km, es_frontera, es_urbano, es_regional, es_aguachica)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            ruta.origen, 
            ruta.destino, 
            ruta.distancia_km,
            1 if ruta.es_frontera else 0,
            1 if ruta.es_urbano else 0,
            1 if ruta.es_regional else 0,
            1 if ruta.es_aguachica else 0
        ))
        conn.commit()
        try:
            ruta_id = cursor.fetchone()[0]
        except:
            try:
                ruta_id = cursor.lastrowid
            except:
                ruta_id = None
        conn.close()
        return ruta_id

    def obtener_rutas(self):
    conn = self.get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, origen, destino, distancia_km, es_frontera, es_urbano, es_regional, es_aguachica FROM rutas ORDER BY origen, destino")
    rutas = []
    for row in cursor.fetchall():
        rutas.append(Ruta(
            origen=row[1],
            destino=row[2],
            distancia_km=row[3],
            es_frontera=bool(row[4]),
            es_urbano=bool(row[5]),
            es_regional=bool(row[6]),
            es_aguachica=bool(row[7])
        ))
    conn.close()
    return rutas

    def eliminar_ruta(self, ruta_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM rutas WHERE id = %s", (ruta_id,))
        conn.commit()
        conn.close()


# ==================== CLASES DE DATOS ====================
@dataclass
class Tractomula:
    placa: str
    consumo_km_galon: float
    tipo: str


@dataclass
class Conductor:
    nombre: str
    cedula: str


@dataclass
class Ruta:
    origen: str
    destino: str
    distancia_km: float
    es_frontera: bool
    es_urbano: bool = False  
    es_regional: bool = False
    es_aguachica: bool = False


# ==================== DATOS COLOMBIANOS ====================
class DatosColombia:
    PRECIO_DIESEL = 10800
    NOMINA_ADMIN_BASE = 1300000
    NOMINA_ADMIN_DIVISOR = 14
    NOMINA_CONDUCTOR_DIA = 20000
    COMISION_FRONTERA = 500000
    COMISION_REGIONAL = 180000
    COMISION_AGUACHICA = 350000
    COMISION_NO_FRONTERA = 100000
    MANTENIMIENTO_MENSUAL = 1500000
    SEGURO_1 = 1400000
    SEGURO_2 = 6000000
    SEGURO_3 = 16000000
    TECNOMECANICA_ANUAL = 460000
    LLANTAS_COSTO = 1300000
    LLANTAS_CANTIDAD = 22
    LLANTAS_KM = 80000
    ACEITE_COSTO = 2500000
    ACEITE_KM = 15000
    CRUCE_FRONTERA = 556000
    PARQUEO_DIA = 15000
    MARGEN_ANT_EMPRESA = 0.90
    DIVISOR_PUNTO_EQUILIBRIO = 0.5


# ==================== ASIGNACION DE CONDUCTORES ====================
PLACA_CONDUCTOR = {
    "NOX459": "HABID CAMACHO",
    "NOX460": "JOSE ORTEGA PEREZ",
    "NOX461": "ISAAC TAFUR",
    "SON047": "ISAIAS VESGA",
    "SON048": "FLAVIO ROSENDO MALTE TUTALCHA",
    "SOP148": "SLITH JOSE ORTEGA PACHECO",
    "SOP149": "ABRAHAM SEGUNDO ALVAREZ VALLE",
    "SOP150": "RAMON TAFUR HERNANDEZ",
    "SRO661": "",
    "SRO672": "PEDRO VILLAMIL",
    "TMW882": "JESUS DAVID MONTE MOSQUERA",
    "TRL282": "CHRISTIAN MARTINEZ NAVARRO",
    "TRL298": "YEIMI DUQUE ZULUAGA",
    "UYQ308": "JULIAN CALETH CORONADO",
    "UYV084": "CARLOS TAFUR",
    "UYY788": "EDUARDO RAFAEL OLIVARES ALCAZAR",
}


# ==================== CALCULADORA DE COSTOS ====================
class CalculadoraCostos:
    """Calcula todos los costos del viaje con fórmulas reales ACTUALIZADAS"""

    def __init__(self, tractomula: Tractomula, conductor: Conductor, ruta: Ruta,
                 dias_viaje: int, es_frontera: bool, hubo_parqueo: bool,
                 flypass: float, peajes: float, hotel: float, comida: float,
                 cargue_descargue: float, otros: float, valor_flete: float,
                 anticipo: float, hubo_anticipo_empresa: bool, datos: DatosColombia):
        self.tractomula = tractomula
        self.conductor = conductor
        self.ruta = ruta
        self.dias_viaje = dias_viaje
        self.es_frontera = es_frontera
        self.hubo_parqueo = hubo_parqueo
        self.flypass = flypass
        self.peajes = peajes
        self.hotel = hotel
        self.comida = comida
        self.cargue_descargue = cargue_descargue
        self.otros = otros
        self.valor_flete = valor_flete
        self.anticipo = anticipo
        self.hubo_anticipo_empresa = hubo_anticipo_empresa
        self.datos = datos

    def calcular_nomina_admin(self) -> float:
        return (self.datos.NOMINA_ADMIN_BASE / self.datos.NOMINA_ADMIN_DIVISOR) * self.dias_viaje

    def calcular_nomina_conductor(self) -> float:
        return self.datos.NOMINA_CONDUCTOR_DIA * self.dias_viaje

    def calcular_comision_conductor(self) -> float:
        if self.ruta.es_aguachica:
            return self.datos.COMISION_AGUACHICA
        elif self.ruta.es_regional:
            return self.datos.COMISION_REGIONAL
        elif self.es_frontera:
            return self.datos.COMISION_FRONTERA
        else:
            return self.datos.COMISION_NO_FRONTERA

    def calcular_mantenimiento(self) -> float:
        return (self.datos.MANTENIMIENTO_MENSUAL / 30) * self.dias_viaje

    def calcular_seguros(self) -> float:
        seguro_diario = (
            (self.datos.SEGURO_1 / 365) +
            (self.datos.SEGURO_2 / 365) +
            (self.datos.SEGURO_3 / 14 / 365)
        )
        return seguro_diario * self.dias_viaje

    def calcular_tecnomecanica(self) -> float:
        return (self.datos.TECNOMECANICA_ANUAL / 365) * self.dias_viaje

    def calcular_llantas(self) -> float:
        costo_por_km = (self.datos.LLANTAS_COSTO * self.datos.LLANTAS_CANTIDAD) / self.datos.LLANTAS_KM
        return costo_por_km * self.ruta.distancia_km

    def calcular_aceite(self) -> float:
        costo_por_km = self.datos.ACEITE_COSTO / self.datos.ACEITE_KM
        return costo_por_km * self.ruta.distancia_km

    def calcular_galones_necesarios(self) -> float:
        if self.tractomula.consumo_km_galon <= 0:
            return 0.0
        return self.ruta.distancia_km / self.tractomula.consumo_km_galon

    def calcular_combustible(self) -> float:
        galones = self.calcular_galones_necesarios()
        return galones * self.datos.PRECIO_DIESEL

    def calcular_cruce_frontera(self) -> float:
        return self.datos.CRUCE_FRONTERA if self.es_frontera else 0

    def calcular_parqueo(self) -> float:
        return self.datos.PARQUEO_DIA * self.dias_viaje if self.hubo_parqueo else 0

    def calcular_legalizacion(self) -> float:
        return (self.peajes + self.calcular_cruce_frontera() + self.hotel +
                self.comida + self.calcular_parqueo() + self.cargue_descargue + self.otros)

    def calcular_saldo(self) -> float:
        legalizacion = self.calcular_legalizacion()
        return self.anticipo - legalizacion

    def calcular_ant_empresa(self) -> float:
        if self.hubo_anticipo_empresa:
            return self.valor_flete * self.datos.MARGEN_ANT_EMPRESA
        else:
            return 0.0

    def calcular_costos_totales(self) -> Dict[str, float]:
        nomina_admin = self.calcular_nomina_admin()
        nomina_conductor = self.calcular_nomina_conductor()
        comision_conductor = self.calcular_comision_conductor()
        mantenimiento = self.calcular_mantenimiento()
        seguros = self.calcular_seguros()
        tecnomecanica = self.calcular_tecnomecanica()
        llantas = self.calcular_llantas()
        aceite = self.calcular_aceite()
        galones_necesarios = self.calcular_galones_necesarios()
        combustible = self.calcular_combustible()
        cruce_frontera = self.calcular_cruce_frontera()
        parqueo = self.calcular_parqueo()

        total_gastos = (
            nomina_admin + nomina_conductor + comision_conductor + mantenimiento +
            seguros + tecnomecanica + llantas + aceite + combustible +
            self.flypass + self.peajes + cruce_frontera + self.hotel +
            self.comida + parqueo + self.cargue_descargue + self.otros
        )

        legalizacion = self.calcular_legalizacion()
        saldo = self.calcular_saldo()
        punto_equilibrio = total_gastos / self.datos.DIVISOR_PUNTO_EQUILIBRIO
        utilidad = self.valor_flete - total_gastos
        rentabilidad = (utilidad / self.valor_flete * 100) if self.valor_flete > 0 else 0
        ant_empresa = self.calcular_ant_empresa()
        saldo_empresa = self.valor_flete - ant_empresa

        return {
            'nomina_admin': round(nomina_admin, 2),
            'nomina_conductor': round(nomina_conductor, 2),
            'comision_conductor': round(comision_conductor, 2),
            'mantenimiento': round(mantenimiento, 2),
            'seguros': round(seguros, 2),
            'tecnomecanica': round(tecnomecanica, 2),
            'llantas': round(llantas, 2),
            'aceite': round(aceite, 2),
            'combustible': round(combustible, 2),
            'galones_necesarios': round(galones_necesarios, 2),
            'cruce_frontera': round(cruce_frontera, 2),
            'parqueo': round(parqueo, 2),
            'total_gastos': round(total_gastos, 2),
            'legalizacion': round(legalizacion, 2),
            'saldo': round(saldo, 2),
            'punto_equilibrio': round(punto_equilibrio, 2),
            'utilidad': round(utilidad, 2),
            'rentabilidad': round(rentabilidad, 2),
            'ant_empresa': round(ant_empresa, 2),
            'saldo_empresa': round(saldo_empresa, 2),
        }


# ==================== GENERADOR DE REPORTES ====================
class GeneradorReportes:
    """Genera reportes detallados de costos"""

    @staticmethod
    def generar_reporte_texto(calculadora: CalculadoraCostos) -> str:
        costos = calculadora.calcular_costos_totales()

        reporte = f"""
{'='*70}
          REPORTE DE COSTOS - TRANSPORTE DE CARGA
{'='*70}
INFORMACIÓN DEL VIAJE
{'-'*70}
Ruta: {calculadora.ruta.origen} → {calculadora.ruta.destino}
Distancia: {formatear_numero(calculadora.ruta.distancia_km)} km
Días del viaje: {calculadora.dias_viaje}
Galones necesarios: {formatear_decimal(costos['galones_necesarios'])} gal
Es frontera: {'Sí' if calculadora.es_frontera else 'No'}
Es regional: {'Sí' if calculadora.ruta.es_regional else 'No'}
Es Aguachica: {'Sí' if calculadora.ruta.es_aguachica else 'No'}
Hubo parqueo: {'Sí' if calculadora.hubo_parqueo else 'No'}

VEHÍCULO
{'-'*70}
Placa: {calculadora.tractomula.placa}
Tipo: {calculadora.tractomula.tipo}
Consumo: {calculadora.tractomula.consumo_km_galon} km/galón

CONDUCTOR
{'-'*70}
Nombre: {calculadora.conductor.nombre}
Cédula: {calculadora.conductor.cedula}

DESGLOSE DE COSTOS
{'='*70}

1. Nómina Admin:          ${formatear_numero(costos['nomina_admin']):>18} COP
2. Nómina Conductor:      ${formatear_numero(costos['nomina_conductor']):>18} COP
3. Comisión Conductor:    ${formatear_numero(costos['comision_conductor']):>18} COP
4. Mantenimiento:         ${formatear_numero(costos['mantenimiento']):>18} COP
5. Seguros:               ${formatear_numero(costos['seguros']):>18} COP
6. Tecnomecánica:         ${formatear_numero(costos['tecnomecanica']):>18} COP
7. Llantas:               ${formatear_numero(costos['llantas']):>18} COP
8. Aceite:                ${formatear_numero(costos['aceite']):>18} COP
9. Combustible:           ${formatear_numero(costos['combustible']):>18} COP
10. Flypass:              ${formatear_numero(calculadora.flypass):>18} COP
11. Peajes:               ${formatear_numero(calculadora.peajes):>18} COP
12. Cruce Frontera:       ${formatear_numero(costos['cruce_frontera']):>18} COP
13. Hotel:                ${formatear_numero(calculadora.hotel):>18} COP
14. Comida:               ${formatear_numero(calculadora.comida):>18} COP
15. Parqueo:              ${formatear_numero(costos['parqueo']):>18} COP
16. Cargue/Descargue:     ${formatear_numero(calculadora.cargue_descargue):>18} COP
17. Otros (engrase, etc): ${formatear_numero(calculadora.otros):>18} COP
{'='*70}

RESULTADOS
{'='*70}
TOTAL GASTOS:             ${formatear_numero(costos['total_gastos']):>18} COP
LEGALIZACIÓN:             ${formatear_numero(costos['legalizacion']):>18} COP
ANTICIPO:                 ${formatear_numero(calculadora.anticipo):>18} COP
SALDO:                    ${formatear_numero(costos['saldo']):>18} COP
PUNTO DE EQUILIBRIO:      ${formatear_numero(costos['punto_equilibrio']):>18} COP
VALOR DEL FLETE:          ${formatear_numero(calculadora.valor_flete):>18} COP
UTILIDAD (UT):            ${formatear_numero(costos['utilidad']):>18} COP
RENTABILIDAD:             {costos['rentabilidad']:>18.1f} %
ANT. EMPRESA (90%):       ${formatear_numero(costos['ant_empresa']):>18} COP
SALDO EMPRESA:            ${formatear_numero(costos['saldo_empresa']):>18} COP

Fecha de generación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*70}
        """
        return reporte

    @staticmethod
    def generar_excel(calculadoras: List[CalculadoraCostos]) -> io.BytesIO:
        """Genera un archivo Excel en memoria para descarga"""
        output = io.BytesIO()
        wb = Workbook()

        # Estilos
        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=11)
        subheader_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        subheader_font = Font(color="FFFFFF", bold=True)
        total_fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
        total_font = Font(bold=True, size=12)
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )

        # Hoja de resumen
        ws_resumen = wb.active
        ws_resumen.title = "Resumen General"

        # Título
        ws_resumen.merge_cells('A1:M1')
        cell = ws_resumen['A1']
        cell.value = "REPORTE DE COSTOS - TRANSPORTE DE CARGA COLOMBIA"
        cell.font = Font(size=14, bold=True, color="1F4E78")
        cell.alignment = Alignment(horizontal='center', vertical='center')

        ws_resumen.merge_cells('A2:M2')
        ws_resumen['A2'] = f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ws_resumen['A2'].alignment = Alignment(horizontal='center')

        # Encabezados
        row = 4
        headers = ['Ruta', 'Placa', 'Conductor', 'Distancia (km)', 'Días', 'Galones',
                   'Combustible', 'Total Gastos', 'Anticipo', 'Saldo', 'Valor Flete',
                   'Utilidad', 'Rentabilidad %']

        for col, header in enumerate(headers, start=1):
            cell = ws_resumen.cell(row=row, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center', vertical='center')
            cell.border = border

        # Datos de cada ruta
        row = 5
        for calc in calculadoras:
            costos = calc.calcular_costos_totales()
            ruta_str = f"{calc.ruta.origen} → {calc.ruta.destino}"

            datos = [
                ruta_str,
                calc.tractomula.placa,
                calc.conductor.nombre,
                calc.ruta.distancia_km,
                calc.dias_viaje,
                costos['galones_necesarios'],
                costos['combustible'],
                costos['total_gastos'],
                calc.anticipo,
                costos['saldo'],
                calc.valor_flete,
                costos['utilidad'],
                costos['rentabilidad']
            ]

            for col, valor in enumerate(datos, start=1):
                cell = ws_resumen.cell(row=row, column=col)
                cell.value = valor
                cell.border = border
                cell.alignment = Alignment(horizontal='center' if col <= 3 else 'right')
                if col == 4:
                    cell.number_format = '#,##0'
                elif col == 5:
                    cell.number_format = '#,##0'
                elif col == 6:
                    cell.number_format = '#,##0.00'
                elif col >= 7 and col <= 12:
                    cell.number_format = '$#,##0'
                elif col == 13:
                    cell.number_format = '#,##0.0"%"'
                else:
                    cell.number_format = '#,##0'

            row += 1

        # Ajustar anchos
        ws_resumen.column_dimensions['A'].width = 30
        ws_resumen.column_dimensions['B'].width = 12
        ws_resumen.column_dimensions['C'].width = 28
        for col in ['D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']:
            ws_resumen.column_dimensions[col].width = 16

        # Hojas detalladas por ruta
        for idx, calc in enumerate(calculadoras, start=1):
            costos = calc.calcular_costos_totales()
            ws = wb.create_sheet(title=f"Ruta {idx}")

            # Título
            ws.merge_cells('A1:D1')
            ws['A1'] = f"{calc.ruta.origen} → {calc.ruta.destino}"
            ws['A1'].font = Font(size=14, bold=True, color="1F4E78")
            ws['A1'].alignment = Alignment(horizontal='center')

            row = 3

            # DESGLOSE DE COSTOS
            ws.merge_cells(f'A{row}:D{row}')
            cell = ws[f'A{row}']
            cell.value = "DESGLOSE DE COSTOS"
            cell.font = subheader_font
            cell.fill = subheader_fill
            row += 1

            ws[f'A{row}'] = "Concepto"
            ws[f'B{row}'] = "Monto (COP)"
            ws[f'A{row}'].font = Font(bold=True)
            ws[f'B{row}'].font = Font(bold=True)
            row += 1

            detalles_costos = [
                ('1. Nómina Admin', costos['nomina_admin']),
                ('2. Nómina Conductor', costos['nomina_conductor']),
                ('3. Comisión Conductor', costos['comision_conductor']),
                ('4. Mantenimiento', costos['mantenimiento']),
                ('5. Seguros', costos['seguros']),
                ('6. Tecnomecánica', costos['tecnomecanica']),
                ('7. Llantas', costos['llantas']),
                ('8. Aceite', costos['aceite']),
                ('9. Combustible', costos['combustible']),
                ('10. Flypass', calc.flypass),
                ('11. Peajes', calc.peajes),
                ('12. Cruce Frontera', costos['cruce_frontera']),
                ('13. Hotel', calc.hotel),
                ('14. Comida', calc.comida),
                ('15. Parqueo', costos['parqueo']),
                ('16. Cargue/Descargue', calc.cargue_descargue),
                ('17. Otros', calc.otros),
            ]

            for concepto, monto in detalles_costos:
                ws[f'A{row}'] = concepto
                ws[f'B{row}'] = monto
                ws[f'B{row}'].number_format = '$#,##0'
                row += 1

            row += 1

            # RESULTADOS
            ws.merge_cells(f'A{row}:D{row}')
            cell = ws[f'A{row}']
            cell.value = "RESULTADOS"
            cell.font = subheader_font
            cell.fill = subheader_fill
            row += 1

            resultados = [
                ('TOTAL GASTOS', costos['total_gastos']),
                ('LEGALIZACIÓN', costos['legalizacion']),
                ('ANTICIPO', calc.anticipo),
                ('SALDO', costos['saldo']),
                ('PUNTO DE EQUILIBRIO', costos['punto_equilibrio']),
                ('', ''),
                ('VALOR DEL FLETE', calc.valor_flete),
                ('UTILIDAD (UT)', costos['utilidad']),
                ('RENTABILIDAD (%)', costos['rentabilidad']),
                ('', ''),
                ('ANT. EMPRESA (90%)', costos['ant_empresa']),
                ('SALDO EMPRESA', costos['saldo_empresa']),
            ]

            for label, value in resultados:
                if label:
                    ws[f'A{row}'] = label
                    ws[f'B{row}'] = value
                    ws[f'A{row}'].font = Font(bold=True)
                    if 'TOTAL' in label or 'UTILIDAD' in label:
                        ws[f'A{row}'].fill = total_fill
                        ws[f'B{row}'].fill = total_fill
                        ws[f'B{row}'].font = total_font
                    if label == 'RENTABILIDAD (%)':
                        ws[f'B{row}'].number_format = '#,##0.0"%"'
                    else:
                        ws[f'B{row}'].number_format = '$#,##0'
                row += 1

            # Ajustar anchos
            ws.column_dimensions['A'].width = 30
            ws.column_dimensions['B'].width = 20
            ws.column_dimensions['C'].width = 15
            ws.column_dimensions['D'].width = 15

        wb.save(output)
        output.seek(0)
        return output

    @staticmethod
    def generar_excel_totales(df_totales: pd.DataFrame) -> io.BytesIO:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_totales.to_excel(writer, sheet_name='Totales por Flota', index=False)
        output.seek(0)
        return output


# ==================== COMPONENTE DE INPUT CON FORMATO ====================
def input_numero(label, value=0.0, min_value=0.0, step=1000.0, key=None, help=None):
    """Input personalizado que acepta formato colombiano"""
    texto = st.text_input(
        label,
        value=formatear_numero(value) if value > 0 else "",
        key=key,
        help=help,
        placeholder="0"
    )

    numero = limpiar_numero(texto) if texto else 0.0

    if numero > 0:
        st.caption(f"💵 {formatear_numero(numero)} COP")

    return numero


# ==================== MANTENER SESIÓN ACTIVA ====================
def mantener_app_activa():
    """Mantiene la aplicación activa mostrando un contador discreto y refrescando automáticamente cada 4 minutos"""
    if 'ultima_actividad' not in st.session_state:
        st.session_state.ultima_actividad = datetime.now()

    tiempo_inactivo = datetime.now() - st.session_state.ultima_actividad
    segundos_inactivo = int(tiempo_inactivo.total_seconds())

    # Auto-refresh cada 4 minutos (240 segundos)
    if segundos_inactivo > 240:
        st.session_state.ultima_actividad = datetime.now()
        st.rerun()

    with st.sidebar:
        st.markdown("---")
        minutos = segundos_inactivo // 60
        segundos = segundos_inactivo % 60
        st.caption(f"⏱️ Sesión activa: {minutos}m {segundos}s")

        if st.button("🔄 Refrescar", key="refresh_manual"):
            st.session_state.ultima_actividad = datetime.now()
            st.rerun()


# ==================== APLICACIÓN PRINCIPAL ====================
def main():
    st.set_page_config(page_title="Calculadora de Costos Transporte - Colombia 2026", layout="wide")
    mantener_app_activa()

    st.title("🚛 Sistema de Cálculo de Costos para Transporte de Carga")
    st.markdown("**Sistema de Gestión de Flotas y Fletes**")

    # Inicializar session state
    if 'datos' not in st.session_state:
        st.session_state.datos = DatosColombia()
    if 'db' not in st.session_state:
        st.session_state.db = DatabaseManager()
    if 'calculadoras' not in st.session_state:
        st.session_state.calculadoras = []

    # Cargar datos de la base de datos
    if 'tractomulas' not in st.session_state:
        st.session_state.tractomulas = st.session_state.db.obtener_tractomulas()
    if 'conductores' not in st.session_state:
        st.session_state.conductores = st.session_state.db.obtener_conductores()
    if 'rutas' not in st.session_state:
        st.session_state.rutas = st.session_state.db.obtener_rutas()

    datos = st.session_state.datos
    db = st.session_state.db

    # Sidebar para configuración global
    with st.sidebar:
        st.header("⚙️ Configuración Global")

        precio_diesel_texto = st.text_input(
            "Precio del Diesel (COP/galón)",
            value=formatear_numero(st.session_state.datos.PRECIO_DIESEL)
        )
        st.session_state.datos.PRECIO_DIESEL = limpiar_numero(precio_diesel_texto)

        st.divider()
        st.subheader("📊 Constantes del Negocio")
        st.caption("Estos valores están fijos según tus fórmulas ACTUALIZADAS")
        st.info(f"""
**Nóminas:**
- Admin base: ${formatear_numero(datos.NOMINA_ADMIN_BASE)} / 14
- Conductor/día: ${formatear_numero(datos.NOMINA_CONDUCTOR_DIA)}

**Comisiones:**
- Aguachica: ${formatear_numero(datos.COMISION_AGUACHICA)}
- Regional: ${formatear_numero(datos.COMISION_REGIONAL)}
- Frontera: ${formatear_numero(datos.COMISION_FRONTERA)}
- Normal: ${formatear_numero(datos.COMISION_NO_FRONTERA)}

**Otros:**
- Tecnomecánica/año: ${formatear_numero(datos.TECNOMECANICA_ANUAL)}
- Llantas: ${formatear_numero(datos.LLANTAS_COSTO)}
- Cruce frontera: ${formatear_numero(datos.CRUCE_FRONTERA)}
- Parqueo/día: ${formatear_numero(datos.PARQUEO_DIA)}
        """)

    # Tabs principales
    tab0, tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Dashboard",
        "1. Tractomulas",
        "2. Rutas",
        "3. Conductores",
        "4. Cálculo de Viaje",
        "5. Reportes",
        "6. 📂 Trazabilidad",
        "7. Acumulado por Flota"
    ])

    with tab0:
        st.header("📊 Dashboard - Resumen de tu Negocio")

        dashboard_data = db.obtener_dashboard_data()
        mes_actual = dashboard_data['mes_actual']

        st.subheader(f"📅 Resumen del Mes - {datetime.now().strftime('%B %Y')}")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "💰 Ingresos Totales",
                f"${formatear_numero(mes_actual['total_ingresos'])}",
                help="Total cobrado a clientes este mes"
            )

        with col2:
            st.metric(
                "💸 Gastos Totales",
                f"${formatear_numero(mes_actual['total_gastos'])}",
                help="Total de gastos operativos"
            )

        with col3:
            utilidad = mes_actual['total_utilidad']
            margen_texto = f"{(utilidad/mes_actual['total_gastos']*100) if mes_actual['total_gastos'] > 0 else 0:.1f}% margen"
            st.metric(
                "📈 Utilidad Neta",
                f"${formatear_numero(utilidad)}",
                delta=margen_texto,
                help="Ingresos - Gastos"
            )

        with col4:
            st.metric(
                "🚛 Viajes Realizados",
                f"{mes_actual['total_viajes']}",
                delta=f"{formatear_numero(mes_actual['total_km'])} km",
                help="Total de viajes este mes"
            )

        # Nuevas métricas UT BRUTA, UT NETA, % UT
        st.divider()
        st.subheader("💎 Indicadores de Utilidad")

        col1, col2, col3 = st.columns(3)

        with col1:
            ut_bruta = dashboard_data['ut_bruta']
            st.metric("UT BRUTA", f"${formatear_numero(ut_bruta)}",
                      help="Suma de todos los TOTAL CXC (Valor Flete)")

        with col2:
            ut_neta = dashboard_data['ut_neta']
            st.metric("UT NETA", f"${formatear_numero(ut_neta)}",
                      help="Suma de TOTAL UTILIDAD")

        with col3:
            porcentaje_ut = dashboard_data['porcentaje_ut']
            color_ut = "🟢" if porcentaje_ut >= 20 else "🟡" if porcentaje_ut >= 10 else "🔴"
            st.metric("% UT", f"{porcentaje_ut:.1f}%",
                      help="% UT = UT NETA / UT BRUTA")
            st.caption(f"{color_ut} Porcentaje de Utilidad sobre Ingresos Brutos")

        if mes_actual['total_viajes'] > 0:
            col1, col2, col3 = st.columns(3)

            with col1:
                margen = (utilidad / mes_actual['total_ingresos'] * 100) if mes_actual['total_ingresos'] > 0 else 0
                color = "🟢" if margen >= 20 else "🟡" if margen >= 10 else "🔴"
                st.info(f"{color} **Margen de Utilidad:** {margen:.1f}%")

            with col2:
                utilidad_promedio = mes_actual['utilidad_promedio']
                st.info(f"💵 **Utilidad Promedio/Viaje:** ${formatear_numero(utilidad_promedio)}")

            with col3:
                ingreso_promedio = mes_actual['total_ingresos'] / mes_actual['total_viajes']
                st.info(f"🎯 **Ingreso Promedio/Viaje:** ${formatear_numero(ingreso_promedio)}")

        st.divider()

        st.subheader("🚛 Rentabilidad por Tractomula")

        if dashboard_data['por_tractomula']:
            tractomula_df = pd.DataFrame(
                dashboard_data['por_tractomula'],
                columns=['Placa', 'Viajes', 'Gastos', 'Ingresos', 'Utilidad']
            )
            tractomula_df['Margen %'] = ((tractomula_df['Utilidad'] / tractomula_df['Ingresos']) * 100).round(1)
            tractomula_df['Gastos'] = tractomula_df['Gastos'].apply(lambda x: f"${formatear_numero(x)}")
            tractomula_df['Ingresos'] = tractomula_df['Ingresos'].apply(lambda x: f"${formatear_numero(x)}")
            tractomula_df['Utilidad'] = tractomula_df['Utilidad'].apply(lambda x: f"${formatear_numero(x)}")

            st.dataframe(tractomula_df, use_container_width=True, hide_index=True)
        else:
            st.info("No hay datos de tractomulas este mes")

        st.divider()
        st.subheader("Totales Acumulados por Unidad")
        placa_seleccionada = st.selectbox("Selecciona una placa", sorted(PLACA_CONDUCTOR.keys()))
        if placa_seleccionada:
            df_totales = db.obtener_totales_por_placa()
            df_placa = df_totales[df_totales['placa'] == placa_seleccionada]
            if not df_placa.empty:
                row = df_placa.iloc[0]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total CXC", f"${formatear_numero(row['total_cxc'])}")
                    st.metric("Total Gastos", f"${formatear_numero(row['total_gastos'])}")
                with col2:
                    st.metric("Total UT", f"${formatear_numero(row['total_ut'])}")
                    st.metric("Rentabilidad", f"{row['total_rentabilidad']:.1f}%")
                with col3:
                    st.metric("Punto Equilibrio", f"${formatear_numero(row['total_punto_equilibrio'])}")
                    st.metric("Saldo Total", f"${formatear_numero(row['total_saldo'])}")
            else:
                st.info("No hay datos para esta placa")

        st.subheader("Comparativa entre Unidades")
        df_totales = db.obtener_totales_por_placa()
        if not df_totales.empty:
            fig_utilidad = px.bar(df_totales, x='placa', y='total_ut', title="Utilidad Total por Unidad")
            st.plotly_chart(fig_utilidad)
            fig_gastos = px.bar(df_totales, x='placa', y='total_gastos', title="Gastos Totales por Unidad")
            st.plotly_chart(fig_gastos)
        else:
            st.info("No hay datos para comparar")

    with tab1:
        st.header("Tus Tractomulas")
        with st.form(key="form_tractomula"):
            col1, col2 = st.columns(2)
            with col1:
                placas_opciones = ['(Escribir nueva)'] + sorted(PLACA_CONDUCTOR.keys())
                placa_seleccion = st.selectbox("Placa", placas_opciones)
                if placa_seleccion == '(Escribir nueva)':
                    placa_ingresada = st.text_input("Placa manual")
                    placa = placa_ingresada.strip().upper()
                else:
                    placa = placa_seleccion

                tipo = st.selectbox("Tipo", ["Sencilla", "Dobletroque", "Minimula", "Otro"])
            with col2:
                consumo_km_galon = st.number_input("Consumo (km/galón)", min_value=0.0, value=2.5)

            submit = st.form_submit_button("Agregar Tractomula")
            if submit and placa:
                tractomula = Tractomula(placa, consumo_km_galon, tipo)
                if db.guardar_tractomula(tractomula):
                    st.session_state.tractomulas = db.obtener_tractomulas()
                    st.success(f"✅ Tractomula {placa} guardada!")
                    st.rerun()
                else:
                    st.error(f"❌ La placa {placa} ya existe")

        if st.session_state.tractomulas:
            st.subheader("Tractomulas Registradas")
            for idx, t in enumerate(st.session_state.tractomulas):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"**{t.placa}** ({t.tipo}, {t.consumo_km_galon} km/gal)")
                with col2:
                    if st.button("🗑️", key=f"eliminar_tractomula_{idx}"):
                        db.eliminar_tractomula(t.placa)
                        st.session_state.tractomulas = db.obtener_tractomulas()
                        st.success(f"Tractomula {t.placa} eliminada")
                        st.rerun()

    with tab2:
        st.header("Tus Rutas")
        with st.form(key="form_ruta"):  # ← CORRECCIÓN: Esta línea debe estar alineada aquí
            col1, col2 = st.columns(2)
            with col1:
                origen = st.text_input("Origen")
                destino = st.text_input("Destino")
                distancia_km = st.number_input("Distancia (km)", min_value=0.0)
            with col2:
                es_frontera = st.checkbox("¿Es ruta a frontera?", help="Activa si el destino es hacia frontera")
                es_urbano = st.checkbox("¿Es urbano?", help="Ruta dentro de ciudad")
                es_regional = st.checkbox("¿Es regional?", help="Comisión conductor: $180,000")
                es_aguachica = st.checkbox("¿Es para Aguachica?", help="Comisión conductor: $350,000")
                ida_vuelta = st.checkbox("Ida y vuelta")

            submit = st.form_submit_button("Agregar Ruta")
            if submit and origen and destino:
                if ida_vuelta:
                    distancia_km *= 2
                    destino = f"{destino} (ida y vuelta)"
                ruta = Ruta(origen, destino, distancia_km, es_frontera, es_urbano, es_regional, es_aguachica)
                ruta_id = db.guardar_ruta(ruta)
                st.session_state.rutas = db.obtener_rutas()
                st.success(f"✅ Ruta {origen} → {destino} guardada!")
                st.rerun()

        if st.session_state.rutas:  # ← CORRECCIÓN: Esta línea debe estar alineada aquí (mismo nivel que with st.form)
            st.subheader("Rutas Registradas")
            conn = st.session_state.db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id, origen, destino, distancia_km, es_frontera, es_urbano, es_regional, es_aguachica FROM rutas ORDER BY origen, destino")
            rutas_con_id = cursor.fetchall()
            conn.close()

            for ruta_data in rutas_con_id:  # ← Esta línea está bien ahora
                ruta_id = ruta_data[0]
                origen = ruta_data[1]
                destino = ruta_data[2]
                dist = ruta_data[3]
                es_front = bool(ruta_data[4])
                es_urb = bool(ruta_data[5]) if len(ruta_data) > 5 else False
                es_reg = bool(ruta_data[6]) if len(ruta_data) > 6 else False
                es_agua = bool(ruta_data[7]) if len(ruta_data) > 7 else False

                col1, col2 = st.columns([4, 1])
                with col1:
                    tags = []
                    if es_front:
                        tags.append("🌐 FRONTERA")
                    if es_urb:
                        tags.append("🏙️ URBANO")
                    if es_reg:
                        tags.append("📍 REGIONAL")
                    if es_agua:
                        tags.append("🏙️ AGUACHICA")
                    tags_str = " ".join(tags)
                    st.write(f"**{origen}** → **{destino}** ({formatear_numero(dist)} km) {tags_str}")
                with col2:
                    if st.button("🗑️", key=f"eliminar_ruta_{ruta_id}"):
                        db.eliminar_ruta(ruta_id)
                        st.session_state.rutas = db.obtener_rutas()
                        st.success("Ruta eliminada")
                        st.rerun()

    with tab3:
        st.header("Tus Conductores")

        if 'conductores_cedulas' not in st.session_state:
            st.session_state.conductores_cedulas = {
                "HABID CAMACHO": "123456789",
                "JOSE ORTEGA PEREZ": "987654321",
                "ISAAC TAFUR": "456789123",
                "ISAIAS VESGA": "789123456",
                "FLAVIO ROSENDO MALTE TUTALCHA": "321654987",
                "SLITH JOSE ORTEGA PACHECO": "654987321",
                "ABRAHAM SEGUNDO ALVAREZ VALLE": "147258369",
                "RAMON TAFUR HERNANDEZ": "369258147",
                "PEDRO VILLAMIL": "258369147",
                "JESUS DAVID MONTE MOSQUERA": "951753486",
                "CHRISTIAN MARTINEZ NAVARRO": "486159753",
                "YEIMI DUQUE ZULUAGA": "753486159",
                "JULIAN CALETH CORONADO": "159753486",
                "CARLOS TAFUR": "357159486",
                "EDUARDO RAFAEL OLIVARES ALCAZAR": "486357159"
            }

        with st.form(key="form_conductor"):
            col1, col2 = st.columns(2)
            with col1:
                nombres_opciones = ['(Escribir nuevo)'] + sorted([n for n in PLACA_CONDUCTOR.values() if n])
                nombre_seleccion = st.selectbox("Nombre", nombres_opciones)
                if nombre_seleccion == '(Escribir nuevo)':
                    nombre = st.text_input("Nombre manual")
                    cedula_auto = ""
                else:
                    nombre = nombre_seleccion
                    cedula_auto = st.session_state.conductores_cedulas.get(nombre, "")
            with col2:
                if cedula_auto:
                    cedula = st.text_input("Cédula", value=cedula_auto)
                    st.info("📋 Cédula encontrada automáticamente")
                else:
                    cedula = st.text_input("Cédula")

            submit = st.form_submit_button("Agregar Conductor")
            if submit and nombre and cedula:
                conductor = Conductor(nombre, cedula)
                if db.guardar_conductor(conductor):
                    st.session_state.conductores = db.obtener_conductores()
                    st.session_state.conductores_cedulas[nombre] = cedula
                    st.success(f"✅ Conductor {nombre} guardado!")
                    st.rerun()
                else:
                    st.error(f"❌ El conductor {nombre} ya existe")

        if st.session_state.conductores:
            st.subheader("Conductores Registrados")
            for idx, c in enumerate(st.session_state.conductores):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.write(f"**{c.nombre}** - Cédula: {c.cedula}")
                with col2:
                    if st.button("🗑️", key=f"eliminar_conductor_{idx}"):
                        db.eliminar_conductor(c.nombre)
                        st.session_state.conductores = db.obtener_conductores()
                        st.success(f"Conductor {c.nombre} eliminado")
                        st.rerun()

    with tab4:
        st.header("Realizar Cálculo de Viaje")
        if not (st.session_state.tractomulas and st.session_state.conductores and st.session_state.rutas):
            st.warning("⚠️ Primero agrega al menos una tractomula, un conductor y una ruta.")
        else:
            with st.form(key="form_calculo"):
                st.subheader("📋 Datos del Viaje")

                col1, col2 = st.columns(2)
                with col1:
                    tractomula_selec = st.selectbox("Selecciona Tractomula", [t.placa for t in st.session_state.tractomulas])
                    tractomula_obj = next(t for t in st.session_state.tractomulas if t.placa == tractomula_selec)

                    conductores = [c.nombre for c in st.session_state.conductores]
                    conductor_asignado = PLACA_CONDUCTOR.get(tractomula_selec)
                    conductor_index = conductores.index(conductor_asignado) if conductor_asignado in conductores else 0
                    conductor_selec = st.selectbox("Selecciona Conductor", conductores, index=conductor_index)
                    conductor_obj = next(c for c in st.session_state.conductores if c.nombre == conductor_selec)

                with col2:
                    ruta_selec = st.selectbox("Selecciona Ruta", [f"{r.origen} → {r.destino}" for r in st.session_state.rutas])
                    ruta_obj = next(r for r in st.session_state.rutas if f"{r.origen} → {r.destino}" == ruta_selec)

                    dias_viaje = st.number_input("Días del viaje", min_value=1, value=1, step=1)

                st.divider()
                st.subheader("📊 Parámetros del Viaje")

                col1, col2, col3 = st.columns(3)
                with col1:
                    es_frontera = st.checkbox("¿Es viaje a frontera?", value=ruta_obj.es_frontera,
                                                help="Afecta Comisión Conductor y Cruce Frontera")
                    hubo_parqueo = st.checkbox("¿Hubo parqueo?", value=False)
                    hubo_anticipo_empresa = st.checkbox("¿Hubo anticipo empresa?", value=False,
                                                       help="Activa ANTICIPO EMPRESA = VALOR FLETE × 0.90")

                with col2:
                    flypass_texto = st.text_input("Flypass (COP)", value="", placeholder="0")
                    flypass = limpiar_numero(flypass_texto)
                    if flypass > 0:
                        st.caption(f"💵 {formatear_numero(flypass)}")

                    peajes_texto = st.text_input("Peajes (COP)", value="", placeholder="0")
                    peajes = limpiar_numero(peajes_texto)
                    if peajes > 0:
                        st.caption(f"💵 {formatear_numero(peajes)}")

                with col3:
                    hotel_texto = st.text_input("Hotel (COP)", value="", placeholder="0")
                    hotel = limpiar_numero(hotel_texto)
                    if hotel > 0:
                        st.caption(f"💵 {formatear_numero(hotel)}")

                    comida_texto = st.text_input("Comida (COP)", value="", placeholder="0")
                    comida = limpiar_numero(comida_texto)
                    if comida > 0:
                        st.caption(f"💵 {formatear_numero(comida)}")

                col1, col2, col3 = st.columns(3)
                with col1:
                    cargue_texto = st.text_input("Cargue/Descargue (COP)", value="", placeholder="0")
                    cargue_descargue = limpiar_numero(cargue_texto)
                    if cargue_descargue > 0:
                        st.caption(f"💵 {formatear_numero(cargue_descargue)}")
                with col2:
                    otros_texto = st.text_input("Otros - Engrase, Lavada, Policía (COP)", value="", placeholder="0")
                    otros = limpiar_numero(otros_texto)
                    if otros > 0:
                        st.caption(f"💵 {formatear_numero(otros)}")
                with col3:
                    anticipo_texto = st.text_input("Anticipo (COP)", value="", placeholder="0",
                                                  help="Anticipo entregado al conductor")
                    anticipo = limpiar_numero(anticipo_texto)
                    if anticipo > 0:
                        st.caption(f"💵 {formatear_numero(anticipo)}")

                st.divider()
                st.subheader("💰 Valor del Flete")

                valor_flete_texto = st.text_input(
                    "💰 Valor del Flete Cobrado al Cliente (COP)",
                    value="",
                    placeholder="Ejemplo: 5.000.000",
                    help="¿Cuánto VAS A COBRAR o YA COBRASTE por este viaje?"
                )
                valor_flete = limpiar_numero(valor_flete_texto)

                if valor_flete > 0:
                    st.success(f"✅ Flete: ${formatear_numero(valor_flete)} COP")

                # Preview de cálculo
                if valor_flete > 0:
                    calc_preview = CalculadoraCostos(
                        tractomula_obj, conductor_obj, ruta_obj,
                        dias_viaje, es_frontera, hubo_parqueo,
                        flypass, peajes, hotel, comida, cargue_descargue, otros,
                        valor_flete, anticipo, hubo_anticipo_empresa, datos
                    )
                    costos_preview = calc_preview.calcular_costos_totales()

                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Total Gastos", f"${formatear_numero(costos_preview['total_gastos'])}")
                    with col2:
                        st.metric("Utilidad", f"${formatear_numero(costos_preview['utilidad'])}",
                                  delta=f"{costos_preview['rentabilidad']:.1f}%")
                    with col3:
                        st.metric("Saldo", f"${formatear_numero(costos_preview['saldo'])}",
                                  help="ANTICIPO - LEGALIZACIÓN")
                    with col4:
                        st.metric("Punto Equilibrio", f"${formatear_numero(costos_preview['punto_equilibrio'])}")

                    observaciones = st.text_area("Observaciones (opcional)", placeholder="Notas sobre este viaje...")

                col_btn1, col_btn2 = st.columns(2)
                with col_btn1:
                    calcular = st.form_submit_button("📊 Calcular Costos", type="primary")
                with col_btn2:
                    guardar = st.form_submit_button("💾 Calcular y Guardar", type="secondary")

                if calcular or guardar:
                    if valor_flete <= 0:
                        st.error("⚠️ Debes ingresar el Valor del Flete para continuar")
                    else:
                        calculadora = CalculadoraCostos(
                            tractomula_obj, conductor_obj, ruta_obj,
                            dias_viaje, es_frontera, hubo_parqueo,
                            flypass, peajes, hotel, comida, cargue_descargue, otros,
                            valor_flete, anticipo, hubo_anticipo_empresa, datos
                        )
                        st.session_state.calculadoras.append(calculadora)

                        if guardar:
                            viaje_id = db.guardar_viaje(calculadora, observaciones)
                            if viaje_id:
                                costos = calculadora.calcular_costos_totales()
                                utilidad = costos.get('utilidad', 0)
                                if utilidad >= 0:
                                    st.success(f"""
                                    ✅ **Viaje guardado exitosamente (ID: {viaje_id})**

                                    - Total Gastos: ${formatear_numero(costos['total_gastos'])}
                                    - Valor Flete: ${formatear_numero(calculadora.valor_flete)}
                                    - **Utilidad: ${formatear_numero(utilidad)}**
                                    - **Rentabilidad: {costos['rentabilidad']:.1f}%**
                                    - **Saldo: ${formatear_numero(costos['saldo'])}**")
                                    """)
                                else:
                                    st.error(f"""
                                    ⚠️ **Viaje guardado (ID: {viaje_id}) - PÉRDIDA DETECTADA**

                                    - Total Gastos: ${formatear_numero(costos['total_gastos'])}
                                    - Valor Flete: ${formatear_numero(calculadora.valor_flete)}
                                    - **Pérdida: ${formatear_numero(utilidad)}**
                                    - **Rentabilidad: {costos['rentabilidad']:.1f}%**

                                    ⚠️ Este viaje NO fue rentable.
                                    """)
                            else:
                                st.error("❌ Error al guardar el viaje en la base de datos.")
                        else:
                            st.success("✅ Cálculo completado! Ve a la pestaña de Reportes.")

    with tab5:
        st.header("📄 Reportes y Descargas")
        if st.session_state.calculadoras:
            for idx, calc in enumerate(st.session_state.calculadoras, 1):
                st.subheader(f"Reporte {idx}: {calc.ruta.origen} → {calc.ruta.destino}")
                st.text(GeneradorReportes.generar_reporte_texto(calc))

            excel_data = GeneradorReportes.generar_excel(st.session_state.calculadoras)
            ultimo = st.session_state.calculadoras[-1]
            conductor_nombre = ultimo.conductor.nombre.strip()
            placa = ultimo.tractomula.placa.strip()
            fecha_archivo = datetime.now().strftime('%d-%m-%Y')
            nombre_archivo = f"{conductor_nombre} {placa} {fecha_archivo}.xlsx"
            nombre_archivo = re.sub(r'[\\/:*?"<>|]', '-', nombre_archivo)

            st.download_button(
                label="📥 Descargar Reporte Completo en Excel",
                data=excel_data,
                file_name=nombre_archivo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            if st.button("🗑️ Limpiar reportes temporales"):
                st.session_state.calculadoras = []
                st.rerun()
        else:
            st.info("Realiza al menos un cálculo en la pestaña anterior para ver reportes.")

    with tab6:
        st.header("📂 Trazabilidad de Viajes")
        st.markdown("Historial completo de todos los viajes guardados en el sistema.")

        with st.expander("🔍 Filtros de Búsqueda", expanded=True):
            col1, col2, col3 = st.columns(3)

            with col1:
                fecha_inicio = st.date_input("Fecha desde", value=None)
                placa_filtro = st.selectbox("Placa", ["Todas"] + sorted(PLACA_CONDUCTOR.keys()))

            with col2:
                fecha_fin = st.date_input("Fecha hasta", value=None)
                conductor_filtro = st.text_input("Conductor (nombre)")
            with col3:
                origen_filtro = st.text_input("Origen")
                destino_filtro = st.text_input("Destino")

            buscar = st.button("🔍 Buscar", type="primary")

        if buscar or 'ultima_busqueda' not in st.session_state:
            fecha_ini = fecha_inicio.strftime('%Y-%m-%d') if fecha_inicio else None
            fecha_fi = fecha_fin.strftime('%Y-%m-%d') if fecha_fin else None
            placa_f = None if placa_filtro == "Todas" else placa_filtro
            conductor_f = conductor_filtro if conductor_filtro else None
            origen_f = origen_filtro if origen_filtro else None
            destino_f = destino_filtro if destino_filtro else None

            df_viajes = db.buscar_viajes(fecha_ini, fecha_fi, placa_f, conductor_f, origen_f, destino_f)
            st.session_state.ultima_busqueda = df_viajes
        else:
            df_viajes = st.session_state.ultima_busqueda

        if df_viajes.empty:
            st.info("No se encontraron viajes con los filtros aplicados.")
        else:
            st.success(f"Se encontraron {len(df_viajes)} viajes")

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Viajes", len(df_viajes))
            with col2:
                st.metric("Kilómetros", f"{formatear_numero(df_viajes['distancia_km'].sum())} km")
            with col3:
                st.metric("Total Gastos", f"${formatear_numero(df_viajes['total_gastos'].sum())}")
            with col4:
                st.metric("Total Utilidad", f"${formatear_numero(df_viajes['utilidad'].sum())}")

            st.subheader("Resultados")

            columnas_mostrar = [
                'id', 'fecha_creacion', 'placa', 'conductor', 'origen', 'destino',
                'distancia_km', 'dias_viaje', 'total_gastos', 'valor_flete',
                'utilidad', 'rentabilidad'
            ]

            df_mostrar = df_viajes[columnas_mostrar].copy()
            df_mostrar.columns = [
                'ID', 'Fecha', 'Placa', 'Conductor', 'Origen', 'Destino',
                'Km', 'Días', 'Total Gastos', 'Valor Flete', 'Utilidad', 'Rentabilidad %'
            ]

            df_mostrar['Total Gastos'] = df_mostrar['Total Gastos'].apply(lambda x: f"${formatear_numero(x)}")
            df_mostrar['Valor Flete'] = df_mostrar['Valor Flete'].apply(lambda x: f"${formatear_numero(x)}")
            df_mostrar['Utilidad'] = df_mostrar['Utilidad'].apply(lambda x: f"${formatear_numero(x)}")
            df_mostrar['Rentabilidad %'] = df_mostrar['Rentabilidad %'].apply(lambda x: f"{x:.1f}%")

            st.dataframe(df_mostrar, use_container_width=True, height=400)

            st.subheader("Ver Detalle de Viaje")
            viaje_id_seleccionado = st.selectbox("Selecciona un viaje por ID", df_viajes['id'].tolist())

            if st.button("Ver Detalle Completo"):
                viaje = db.obtener_viaje_por_id(viaje_id_seleccionado)
                if viaje:
                    col1, col2 = st.columns(2)

                    with col1:
                        st.markdown("### 📋 Información del Viaje")
                        st.write(f"**ID:** {viaje[0]}")
                        st.write(f"**Fecha:** {viaje[1]}")
                        st.write(f"**Placa:** {viaje[2]}")
                        st.write(f"**Conductor:** {viaje[3]}")
                        st.write(f"**Ruta:** {viaje[4]} → {viaje[5]}")
                        st.write(f"**Distancia:** {formatear_numero(viaje[6])} km")
                        st.write(f"**Días:** {viaje[7]}")
                        st.write(f"**Es Frontera:** {'Sí' if viaje[8] else 'No'}")
                        st.write(f"**Hubo Parqueo:** {'Sí' if viaje[9] else 'No'}")

                    with col2:
                        st.markdown("### 💰 Resultados Financieros")
                        st.write(f"**Total Gastos:** ${formatear_numero(viaje[28])}")
                        st.write(f"**Legalización:** ${formatear_numero(viaje[29])}")
                        st.write(f"**Punto Equilibrio:** ${formatear_numero(viaje[30])}")
                        st.write(f"**Valor Flete:** ${formatear_numero(viaje[31])}")

                        utilidad = viaje[32] if viaje[32] is not None else 0
                        rentabilidad = viaje[33] if viaje[33] is not None else 0

                        if utilidad >= 0:
                            st.success(f"**✅ Utilidad:** ${formatear_numero(utilidad)}")
                            st.success(f"**Rentabilidad:** {rentabilidad:.1f}%")
                        else:
                            st.error(f"**⚠️ Pérdida:** ${formatear_numero(utilidad)}")
                            st.error(f"**Rentabilidad:** {rentabilidad:.1f}%")

                        st.write(f"**Anticipo:** ${formatear_numero(viaje[34])}")
                        st.write(f"**Saldo:** ${formatear_numero(viaje[35])}")
                        st.write(f"**Hubo Anticipo Empresa:** {'Sí' if viaje[36] else 'No'}")
                        st.write(f"**Ant. Empresa (90%):** ${formatear_numero(viaje[37])}")
                        st.write(f"**Saldo Empresa:** ${formatear_numero(viaje[38])}")

                    st.markdown("### 📊 Desglose de Costos")
                    col1, col2, col3 = st.columns(3)

                    with col1:
                        st.write(f"1. Nómina Admin: ${formatear_numero(viaje[10])}")
                        st.write(f"2. Nómina Conductor: ${formatear_numero(viaje[11])}")
                        st.write(f"3. Comisión Conductor: ${formatear_numero(viaje[12])}")
                        st.write(f"4. Mantenimiento: ${formatear_numero(viaje[13])}")
                        st.write(f"5. Seguros: ${formatear_numero(viaje[14])}")
                        st.write(f"6. Tecnomecánica: ${formatear_numero(viaje[15])}")
                        st.write(f"7. Llantas: ${formatear_numero(viaje[16])}")
                        st.write(f"8. Aceite: ${formatear_numero(viaje[17])}")
                        st.write(f"9. Combustible: ${formatear_numero(viaje[18])}")
                        st.write(f"10. Flypass: ${formatear_numero(viaje[20])}")
                        st.write(f"11. Peajes: ${formatear_numero(viaje[21])}")
                        st.write(f"12. Cruce Frontera: ${formatear_numero(viaje[22])}")
                        st.write(f"13. Hotel: ${formatear_numero(viaje[23])}")
                        st.write(f"14. Comida: ${formatear_numero(viaje[24])}")
                        st.write(f"15. Parqueo: ${formatear_numero(viaje[25])}")
                        st.write(f"16. Cargue/Descargue: ${formatear_numero(viaje[26])}")
                        st.write(f"17. Otros: ${formatear_numero(viaje[27])}")

                    with col2:
                        st.write(f"7. Llantas: ${formatear_numero(viaje[16])}")
                        st.write(f"8. Aceite: ${formatear_numero(viaje[17])}")
                        st.write(f"9. Combustible: ${formatear_numero(viaje[18])}")
                        st.write(f" - Galones: {formatear_decimal(viaje[19])}")
                        st.write(f"10. Flypass: ${formatear_numero(viaje[20])}")
                        st.write(f"11. Peajes: ${formatear_numero(viaje[21])}")

                    with col3:
                        st.write(f"12. Cruce Frontera: ${formatear_numero(viaje[22])}")
                        st.write(f"13. Hotel: ${formatear_numero(viaje[23])}")
                        st.write(f"14. Comida: ${formatear_numero(viaje[24])}")
                        st.write(f"15. Parqueo: ${formatear_numero(viaje[25])}")
                        st.write(f"16. Cargue/Descargue: ${formatear_numero(viaje[26])}")
                        st.write(f"17. Otros: ${formatear_numero(viaje[27])}")

                    if viaje[39]:
                        st.markdown("### 📝 Observaciones")
                        st.info(viaje[39])

                    if st.button("🗑️ Eliminar este viaje", type="secondary"):
                        db.eliminar_viaje(viaje_id_seleccionado)
                        st.success("Viaje eliminado")
                        st.rerun()

            st.subheader("📥 Exportar Resultados")
            if st.button("Descargar en Excel"):
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df_viajes.to_excel(writer, sheet_name='Viajes', index=False)

                output.seek(0)
                fecha_descarga = datetime.now().strftime('%Y-%m-%d')
                st.download_button(
                    label="📥 Descargar Historial en Excel",
                    data=output,
                    file_name=f"Historial_Viajes_{fecha_descarga}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        st.subheader("📊 Estadísticas Generales")
        stats = db.obtener_estadisticas()

        col1, col2, col3 = st.columns(3)

        with col1:
            st.markdown("### Resumen Global")
            st.metric("Total de Viajes", stats['total_viajes'])
            st.metric("Total Kilómetros", f"{formatear_numero(stats['total_km'])} km")
            st.metric("Gastos Acumulados", f"${formatear_numero(stats['total_gastos'])}")

        with col2:
            st.markdown("### Top Tractomulas")
            if stats['viajes_por_placa']:
                for placa, total in stats['viajes_por_placa'][:5]:
                    st.write(f"**{placa}:** {total} viajes")
            else:
                st.info("No hay datos")

        with col3:
            st.markdown("### Top Conductores")
            if stats['viajes_por_conductor']:
                for conductor, total in stats['viajes_por_conductor'][:5]:
                    st.write(f"**{conductor}:** {total} viajes")
            else:
                st.info("No hay datos")

        if stats['rutas_frecuentes']:
            st.markdown("### Rutas Más Frecuentes")
            rutas_df = pd.DataFrame(stats['rutas_frecuentes'], columns=['Origen', 'Destino', 'Cantidad'])
            st.dataframe(rutas_df, use_container_width=True, hide_index=True)

    with tab7:
        st.header("Acumulado por Flota")
        st.markdown("Acumulados totales por unidad (tractomula/placa)")

        with st.expander("🔍 Filtros por Fecha", expanded=True):
            filtro_tipo = st.selectbox("Tipo de Filtro", ["Ninguno", "Mes", "Año", "Rango Personalizado"])
            fecha_inicio = None
            fecha_fin = None
            if filtro_tipo == "Mes":
                mes_seleccionado = st.selectbox("Mes", range(1, 13))
                año_seleccionado = st.selectbox("Año", range(2020, datetime.now().year + 1))
                fecha_inicio = f"{año_seleccionado}-{mes_seleccionado:02d}-01"
                ultimo_dia = (datetime(año_seleccionado, mes_seleccionado + 1, 1) - timedelta(days=1)).day
                fecha_fin = f"{año_seleccionado}-{mes_seleccionado:02d}-{ultimo_dia}"
            elif filtro_tipo == "Año":
                año_seleccionado = st.selectbox("Año", range(2020, datetime.now().year + 1))
                fecha_inicio = f"{año_seleccionado}-01-01"
                fecha_fin = f"{año_seleccionado}-12-31"
            elif filtro_tipo == "Rango Personalizado":
                col1, col2 = st.columns(2)
                with col1:
                    fecha_inicio = st.date_input("Desde", value=None)
                with col2:
                    fecha_fin = st.date_input("Hasta", value=None)
                fecha_inicio = fecha_inicio.strftime('%Y-%m-%d') if fecha_inicio else None
                fecha_fin = fecha_fin.strftime('%Y-%m-%d') if fecha_fin else None

            buscar_totales = st.button("Aplicar Filtro", type="primary")

        if buscar_totales or 'ultimos_totales' not in st.session_state:
            df_totales = db.obtener_totales_por_placa(fecha_inicio, fecha_fin)
            st.session_state.ultimos_totales = df_totales
        else:
            df_totales = st.session_state.ultimos_totales

        if df_totales.empty:
            st.info("No hay datos con los filtros aplicados.")
        else:
            df_mostrar = df_totales.copy()
            for col in df_mostrar.columns:
                if col != 'placa' and col.startswith('total_'):
                    if 'rentabilidad' in col:
                        df_mostrar[col] = df_mostrar[col].apply(lambda x: f"{x:.1f}%")
                    else:
                        df_mostrar[col] = df_mostrar[col].apply(lambda x: f"${formatear_numero(x)}")

            st.dataframe(df_mostrar, use_container_width=True)

            excel_totales = GeneradorReportes.generar_excel_totales(df_totales)
            st.download_button(
                label="📥 Descargar Reporte en Excel",
                data=excel_totales,
                file_name=f"Acumulados_Flota_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            st.subheader("Gráficos Comparativos")
            fig_cxc = px.bar(df_totales, x='placa', y='total_cxc', title="Total CXC por Unidad")
            st.plotly_chart(fig_cxc)
            fig_gastos = px.bar(df_totales, x='placa', y='total_gastos', title="Total Gastos por Unidad")
            st.plotly_chart(fig_gastos)
            fig_ut = px.bar(df_totales, x='placa', y='total_ut', title="Total UT por Unidad")
            st.plotly_chart(fig_ut)
            fig_rentabilidad = px.bar(df_totales, x='placa', y='total_rentabilidad', title="Rentabilidad por Unidad")
            st.plotly_chart(fig_rentabilidad)

if __name__ == "__main__":
    main()

