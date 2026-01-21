"""
Sistema de Programación de Rutas y Cálculo de Costos para Tractomulas
Versión 3.3 - Conectado a Supabase (PostgreSQL) - ACTUALIZADO
Contexto: Colombia
Autor: Sistema de Gestión de Transporte de Carga
"""

import streamlit as st
import json
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from typing import List, Dict, Tuple
import math
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
        texto = str(texto).replace('.', '').replace(',', '.')
        return float(texto)
    except:
        return 0.0


# ==================== BASE DE DATOS SUPABASE (POSTGRES) ====================
class DatabaseManager:
    """Gestor de base de datos Supabase (PostgreSQL) para trazabilidad"""

    def __init__(self):
        self.db_url = SUPABASE_DB_URL
        self.init_database()

    def get_connection(self):
        return psycopg2.connect(self.db_url)

    def init_database(self):
        """Crea las tablas si no existen (Sintaxis PostgreSQL)"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # Tabla de viajes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS viajes (
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

    def guardar_viaje(self, calculadora, observaciones=""):
        """Guarda un viaje en la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        costos = calculadora.calcular_costos_totales()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # CORRECCIÓN: Asegurar que el número de %s coincida exactamente con las columnas (39)
        cursor.execute('''
            INSERT INTO viajes (
                fecha_creacion, placa, conductor, origen, destino, distancia_km,
                dias_viaje, es_frontera, hubo_parqueo, nomina_admin, nomina_conductor,
                comision_conductor, mantenimiento, seguros, tecnomecanica, llantas,
                aceite, combustible, galones_necesarios, flypass, peajes,
                cruce_frontera, hotel, comida, parqueo, cargue_descargue, otros,
                total_gastos, legalizacion, punto_equilibrio, valor_flete,
                utilidad, rentabilidad, anticipo, saldo, hubo_anticipo_empresa,
                ant_empresa, saldo_empresa, observaciones
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            fecha_actual,
            calculadora.tractomula.placa,
            calculadora.conductor.nombre,
            calculadora.ruta.origen,
            calculadora.ruta.destino,
            calculadora.ruta.distancia_km,
            calculadora.dias_viaje,
            1 if calculadora.es_frontera else 0,
            1 if calculadora.hubo_parqueo else 0,
            costos['nomina_admin'],
            costos['nomina_conductor'],
            costos['comision_conductor'],
            costos['mantenimiento'],
            costos['seguros'],
            costos['tecnomecanica'],
            costos['llantas'],
            costos['aceite'],
            costos['combustible'],
            costos['galones_necesarios'],
            calculadora.flypass,
            calculadora.peajes,
            costos['cruce_frontera'],
            calculadora.hotel,
            calculadora.comida,
            costos['parqueo'],
            calculadora.cargue_descargue,
            calculadora.otros,
            costos['total_gastos'],
            costos['legalizacion'],
            costos['punto_equilibrio'],
            calculadora.valor_flete,
            costos['utilidad'],
            costos['rentabilidad'],
            calculadora.anticipo,
            costos['saldo'],
            1 if calculadora.hubo_anticipo_empresa else 0,
            costos['ant_empresa'],
            costos['saldo_empresa'],
            observaciones
        ))

        conn.commit()
        # En Postgres moderno es mejor usar RETURNING id, pero por compatibilidad:
        try:
            viaje_id = cursor.fetchone()[0]
        except:
             viaje_id = cursor.lastrowid # Puede fallar en Postgres 12+, pero es fallback
        conn.close()
        return viaje_id

    def obtener_todos_viajes(self):
        conn = self.get_connection()
        query = "SELECT * FROM viajes ORDER BY fecha_creacion DESC"
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def buscar_viajes(self, fecha_inicio=None, fecha_fin=None, placa=None, conductor=None, origen=None, destino=None):
        conn = self.get_connection()
        query = "SELECT * FROM viajes WHERE 1=1"
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
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM viajes WHERE id = %s", (viaje_id,))
        viaje = cursor.fetchone()
        conn.close()
        return viaje

    def eliminar_viaje(self, viaje_id):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM viajes WHERE id = %s", (viaje_id,))
        conn.commit()
        conn.close()

    def obtener_estadisticas(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        stats = {}
        cursor.execute("SELECT COUNT(*) FROM viajes")
        stats['total_viajes'] = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(distancia_km) FROM viajes")
        stats['total_km'] = cursor.fetchone()[0] or 0
        cursor.execute("SELECT SUM(total_gastos) FROM viajes")
        stats['total_gastos'] = cursor.fetchone()[0] or 0
        cursor.execute("SELECT placa, COUNT(*) as total FROM viajes GROUP BY placa ORDER BY total DESC")
        stats['viajes_por_placa'] = cursor.fetchall()
        cursor.execute("SELECT conductor, COUNT(*) as total FROM viajes GROUP BY conductor ORDER BY total DESC")
        stats['viajes_por_conductor'] = cursor.fetchall()
        cursor.execute("SELECT origen, destino, COUNT(*) as total FROM viajes GROUP BY origen, destino ORDER BY total DESC LIMIT 5")
        stats['rutas_frecuentes'] = cursor.fetchall()
        conn.close()
        return stats

    def obtener_dashboard_data(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        hoy = datetime.now()
        inicio_mes = hoy.replace(day=1).strftime('%Y-%m-%d')
        data = {}

        cursor.execute("""
            SELECT COUNT(*) as total_viajes, SUM(distancia_km) as total_km,
                   SUM(total_gastos) as total_gastos, SUM(valor_flete) as total_ingresos,
                   SUM(utilidad) as total_utilidad, AVG(utilidad) as utilidad_promedio
            FROM viajes
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
            FROM viajes
            WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
            GROUP BY placa ORDER BY utilidad DESC
        """, (inicio_mes,))
        data['por_tractomula'] = cursor.fetchall()

        cursor.execute("""
            SELECT conductor, COUNT(*) as viajes, SUM(utilidad) as utilidad,
                   AVG(utilidad) as utilidad_promedio
            FROM viajes
            WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
            GROUP BY conductor ORDER BY utilidad DESC
        """, (inicio_mes,))
        data['por_conductor'] = cursor.fetchall()

        cursor.execute("""
            SELECT origen, destino, COUNT(*) as viajes, AVG(utilidad) as utilidad_promedio,
                   SUM(utilidad) as utilidad_total
            FROM viajes
            WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
            GROUP BY origen, destino ORDER BY utilidad_total DESC LIMIT 5
        """, (inicio_mes,))
        data['rutas_rentables'] = cursor.fetchall()

        cursor.execute("""
            SELECT to_char(to_date(fecha_creacion, 'YYYY-MM-DD'), 'YYYY-MM') as mes,
                   COUNT(*) as viajes, SUM(total_gastos) as gastos,
                   SUM(valor_flete) as ingresos, SUM(utilidad) as utilidad
            FROM viajes
            WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '6 months'
            GROUP BY mes ORDER BY mes
        """)
        data['evolucion_6_meses'] = cursor.fetchall()

        cursor.execute("""
            SELECT fecha_creacion, placa, origen, destino, total_gastos, valor_flete, utilidad
            FROM viajes WHERE utilidad < 0 ORDER BY fecha_creacion DESC LIMIT 10
        """)
        data['viajes_no_rentables'] = cursor.fetchall()

        cursor.execute("""
            SELECT SUM(valor_flete) as ut_bruta, SUM(utilidad) as ut_neta
            FROM viajes WHERE to_date(fecha_creacion, 'YYYY-MM-DD') >= %s
        """, (inicio_mes,))
        row_ut = cursor.fetchone()
        ut_bruta = row_ut[0] or 0
        ut_neta = row_ut[1] or 0
        porcentaje_ut = (ut_neta / ut_bruta * 100) if ut_bruta > 0 else 0
        data['ut_bruta'] = ut_bruta
        data['ut_neta'] = ut_neta
        data['porcentaje_ut'] = porcentaje_ut

        conn.close()
        return data

    def obtener_totales_por_placa(self, fecha_inicio=None, fecha_fin=None):
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
            FROM viajes WHERE 1=1
        """
        params = []
        if fecha_inicio:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') <= %s"
            params.append(fecha_fin)
        query += " GROUP BY placa ORDER BY placa"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

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

        return df

    # Métodos para tractomulas
    def guardar_tractomula(self, tractomula: 'Tractomula'):
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
    def guardar_conductor(self, conductor: 'Conductor'):
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
    def guardar_ruta(self, ruta: 'Ruta'):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO rutas (origen, destino, distancia_km, es_frontera, es_regional, es_aguachica)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', (
            ruta.origen, ruta.destino, ruta.distancia_km,
            1 if ruta.es_frontera else 0,
            1 if ruta.es_regional else 0,
            1 if ruta.es_aguachica else 0
        ))
        conn.commit()
        ruta_id = cursor.lastrowid
        conn.close()
        return ruta_id

    def obtener_rutas(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM rutas ORDER BY origen, destino")
        rutas = []
        for row in cursor.fetchall():
            rutas.append(Ruta(
                origen=row[1],
                destino=row[2],
                distancia_km=row[3],
                es_frontera=bool(row[4]),
                es_regional=bool(row[5]) if len(row) > 5 else False,
                es_aguachica=bool(row[6]) if len(row) > 6 else False
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
        output = io.BytesIO()
        wb = Workbook()
        header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
        header_font = Font(color="FFFFFF", bold=True, size=11)
        subheader_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
        subheader_font = Font(color="FFFFFF", bold=True)
        total_fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
        total_font = Font(bold=True, size=12)
        border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

        ws_resumen = wb.active
        ws_resumen.title = "Resumen General"
        ws_resumen.merge_cells('A1:M1')
        cell = ws_resumen['A1']
        cell.value = "REPORTE DE COSTOS - TRANSPORTE DE CARGA COLOMBIA"
        cell.font = Font(size=14, bold=True, color="1F4E78")
        cell.alignment = Alignment(horizontal='center', vertical='center')
        ws_resumen.merge_cells('A2:M2')
        ws_resumen['A2'] = f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ws_resumen['A2'].alignment = Alignment(horizontal='center')

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

        row = 5
        for calc in calculadoras:
            costos = calc.calcular_costos_totales()
            ruta_str = f"{calc.ruta.origen} → {calc.ruta.destino}"
            datos = [
                ruta_str, calc.tractomula.placa, calc.conductor.nombre, calc.ruta.distancia_km,
                calc.dias_viaje, costos['galones_necesarios'], costos['combustible'],
                costos['total_gastos'], calc.anticipo, costos['saldo'], calc.valor_flete,
                costos['utilidad'], costos['rentabilidad']
            ]
            for col, valor in enumerate(datos, start=1):
                cell = ws_resumen.cell(row=row, column=col)
                cell.value = valor
                cell.border = border
                cell.alignment = Alignment(horizontal='center' if col <= 3 else 'right')
                if col == 4: cell.number_format = '#,##0'
                elif col == 5: cell.number_format = '#,##0'
                elif col == 6: cell.number_format = '#,##0.00'
                elif col >= 7 and col <= 12: cell.number_format = '$#,##0'
                elif col == 13: cell.number_format = '#,##0.0"%"'
            row += 1

        ws_resumen.column_dimensions['A'].width = 30
        ws_resumen.column_dimensions['B'].width = 12
        ws_resumen.column_dimensions['C'].width = 28
        for col in ['D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M']:
            ws_resumen.column_dimensions[col].width = 16

        for idx, calc in enumerate(calculadoras, start=1):
            costos = calc.calcular_costos_totales()
            ws = wb.create_sheet(title=f"Ruta {idx}")
            ws.merge_cells('A1:D1')
            ws['A1'] = f"{calc.ruta.origen} → {calc.ruta.destino}"
            ws['A1'].font = Font(size=14, bold=True, color="1F4E78")
            ws['A1'].alignment = Alignment(horizontal='center')
            row = 3
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
                    if label == 'RENTABILIDAD (%)': ws[f'B{row}'].number_format = '#,##0.0"%"'
                    else: ws[f'B{row}'].number_format = '$#,##0'
                row += 1
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
    texto = st.text_input(label, value=formatear_numero(value) if value > 0 else "", key=key, help=help, placeholder="0")
    numero = limpiar_numero(texto) if texto else 0.0
    if numero > 0: st.caption(f"💵 {formatear_numero(numero)} COP")
    return numero


# ==================== MANTENER SESIÓN ACTIVA ====================
def mantener_app_activa():
    if 'ultima_actividad' not in st.session_state: st.session_state.ultima_actividad = datetime.now()
    tiempo_inactivo = datetime.now() - st.session_state.ultima_actividad
    segundos_inactivo = int(tiempo_inactivo.total_seconds())
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

    if 'datos' not in st.session_state: st.session_state.datos = DatosColombia()
    if 'db' not in st.session_state: st.session_state.db = DatabaseManager()
    if 'calculadoras' not in st.session_state: st.session_state.calculadoras = []

    if 'tractomulas' not in st.session_state: st.session_state.tractomulas = st.session_state.db.obtener_tractomulas()
    if 'conductores' not in st.session_state: st.session_state.conductores = st.session_state.db.obtener_conductores()
    if 'rutas' not in st.session_state: st.session_state.rutas = st.session_state.db.obtener_rutas()

    datos = st.session_state.datos
    db = st.session_state.db

    with st.sidebar:
        st.header("⚙️ Configuración Global")
        precio_diesel_texto = st.text_input("Precio del Diesel (COP/galón)", value=formatear_numero(st.session_state.datos.PRECIO_DIESEL))
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

    tab0, tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "📊 Dashboard", "1. Tractomulas", "2. Rutas", "3. Conductores",
        "4. Cálculo de Viaje", "5. Reportes", "6. 📂 Trazabilidad", "7. Acumulado por Flota"
    ])

    with tab0:
        st.header("📊 Dashboard - Resumen de tu Negocio")
        dashboard_data = db.obtener_dashboard_data()
        mes_actual = dashboard_data['mes_actual']
        st.subheader(f"📅 Resumen del Mes - {datetime.now().strftime('%B %Y')}")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("💰 Ingresos Totales", f"${formatear_numero(mes_actual['total_ingresos'])}")
        with col2:
            st.metric("💸 Gastos Totales", f"${formatear_numero(mes_actual['total_gastos'])}")
        with col3:
            utilidad = mes_actual['total_utilidad']
            st.metric("📈 Utilidad Neta", f"${formatear_numero(utilidad)}")
        with col4:
            st.metric("🚛 Viajes Realizados", f"{mes_actual['total_viajes']}")
        st.divider()
        st.subheader("💎 Indicadores de Utilidad")
        col1, col2, col3 = st.columns(3)
        with col1: st.metric("UT BRUTA", f"${formatear_numero(dashboard_data['ut_bruta'])}")
        with col2: st.metric("UT NETA", f"${formatear_numero(dashboard_data['ut_neta'])}")
        with col3:
            porcentaje_ut = dashboard_data['porcentaje_ut']
            color_ut = "🟢" if porcentaje_ut >= 20 else "🟡" if porcentaje_ut >= 10 else "🔴"
            st.metric("% UT", f"{porcentaje_ut:.1f}%")
            st.caption(f"{color_ut} Porcentaje de Utilidad sobre Ingresos Brutos")

        if mes_actual['total_viajes'] > 0:
            col1, col2, col3 = st.columns(3)
            with col1: st.info(f"🟢 **Margen de Utilidad:** {(utilidad/mes_actual['total_ingresos']*100) if mes_actual['total_ingresos'] > 0 else 0:.1f}%")
            with col2: st.info(f"💵 **Utilidad Promedio/Viaje:** ${formatear_numero(mes_actual['utilidad_promedio'])}")
            with col3: st.info(f"🎯 **Ingreso Promedio/Viaje:** ${formatear_numero(mes_actual['total_ingresos']/mes_actual['total_viajes'])}")

        st.divider()
        st.subheader("🚛 Rentabilidad por Tractomula")
        if dashboard_data['por_tractomula']:
            tractomula_df = pd.DataFrame(dashboard_data['por_tractomula'], columns=['Placa', 'Viajes', 'Gastos', 'Ingresos', 'Utilidad'])
            tractomula_df['Margen %'] = ((tractomula_df['Utilidad'] / tractomula_df['Ingresos']) * 100).round(1)
            st.dataframe(tractomula_df, use_container_width=True, hide_index=True)
        
        st.subheader("Comparativa entre Unidades")
        df_totales = db.obtener_totales_por_placa()
        if not df_totales.empty:
            fig_utilidad = px.bar(df_totales, x='placa', y='total_ut', title="Utilidad Total por Unidad")
            st.plotly_chart(fig_utilidad)

    with tab1:
        st.header("Tus Tractomulas")
        with st.form(key="form_tractomula"):
            col1, col2 = st.columns(2)
            with col1:
                placas_opciones = ['(Escribir nueva)'] + sorted(PLACA_CONDUCTOR.keys())
                placa_seleccion = st.selectbox("Placa", placas_opciones)
                placa = placa_seleccion if placa_seleccion != '(Escribir nueva)' else st.text_input("Placa manual").strip().upper()
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
                else: st.error(f"❌ La placa {placa} ya existe")

        if st.session_state.tractomulas:
            st.subheader("Tractomulas Registradas")
            for idx, t in enumerate(st.session_state.tractomulas):
                col1, col2 = st.columns([4, 1])
                with col1: st.write(f"**{t.placa}** ({t.tipo}, {t.consumo_km_galon} km/gal)")
                with col2:
                    if st.button("🗑️", key=f"eliminar_tractomula_{idx}"):
                        db.eliminar_tractomula(t.placa)
                        st.session_state.tractomulas = db.obtener_tractomulas()
                        st.success(f"Tractomula {t.placa} eliminada")
                        st.rerun()

    with tab2:
        st.header("Tus Rutas")
        with st.form(key="form_ruta"):
            col1, col2 = st.columns(2)
            with col1:
                origen = st.text_input("Origen")
                destino = st.text_input("Destino")
                distancia_km = st.number_input("Distancia (km)", min_value=0.0)
            with col2:
                es_frontera = st.checkbox("¿Es ruta a frontera?")
                es_regional = st.checkbox("¿Es regional?")
                es_aguachica = st.checkbox("¿Es para Aguachica?")
                ida_vuelta = st.checkbox("Ida y vuelta")
            submit = st.form_submit_button("Agregar Ruta")
            if submit and origen and destino:
                if ida_vuelta: distancia_km *= 2; destino = f"{destino} (ida y vuelta)"
                ruta = Ruta(origen, destino, distancia_km, es_frontera, es_regional, es_aguachica)
                ruta_id = db.guardar_ruta(ruta)
                st.session_state.rutas = db.obtener_rutas()
                st.success(f"✅ Ruta {origen} → {destino} guardada!")
                st.rerun()

        if st.session_state.rutas:
            st.subheader("Rutas Registradas")
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT id, origen, destino, distancia_km, es_frontera, es_regional, es_aguachica FROM rutas ORDER BY origen, destino")
            rutas_con_id = cursor.fetchall()
            conn.close()
            for ruta_data in rutas_con_id:
                ruta_id, origen, destino, dist, es_front, es_reg, es_agua = ruta_data[0], ruta_data[1], ruta_data[2], ruta_data[3], bool(ruta_data[4]), bool(ruta_data[5]) if len(ruta_data) > 5 else False, bool(ruta_data[6]) if len(ruta_data) > 6 else False
                col1, col2 = st.columns([4, 1])
                with col1:
                    tags = " ".join([t for t, cond in [("🌐 FRONTERA", es_front), ("📍 REGIONAL", es_reg), ("🏙️ AGUACHICA", es_agua)] if cond])
                    st.write(f"**{origen}** → **{destino}** ({formatear_numero(dist)} km) {tags}")
                with col2:
                    if st.button("🗑️", key=f"eliminar_ruta_{ruta_id}"):
                        db.eliminar_ruta(ruta_id)
                        st.session_state.rutas = db.obtener_rutas()
                        st.success("Ruta eliminada")
                        st.rerun()

    with tab3:
        st.header("Tus Conductores")
        if 'conductores_cedulas' not in st.session_state:
            st.session_state.conductores_cedulas = {n: "123456789" for n in PLACA_CONDUCTOR.values() if n}
        with st.form(key="form_conductor"):
            col1, col2 = st.columns(2)
            with col1:
                nombres_opciones = ['(Escribir nuevo)'] + sorted([n for n in PLACA_CONDUCTOR.values() if n])
                nombre_seleccion = st.selectbox("Nombre", nombres_opciones)
                nombre = nombre_seleccion if nombre_seleccion != '(Escribir nuevo)' else st.text_input("Nombre manual")
                cedula_auto = st.session_state.conductores_cedulas.get(nombre, "") if nombre_seleccion != '(Escribir nuevo)' else ""
            with col2:
                cedula = st.text_input("Cédula", value=cedula_auto)
                if cedula_auto: st.info("📋 Cédula encontrada automáticamente")
            submit = st.form_submit_button("Agregar Conductor")
            if submit and nombre and cedula:
                conductor = Conductor(nombre, cedula)
                if db.guardar_conductor(conductor):
                    st.session_state.conductores = db.obtener_conductores()
                    st.session_state.conductores_cedulas[nombre] = cedula
                    st.success(f"✅ Conductor {nombre} guardado!")
                    st.rerun()
                else: st.error(f"❌ El conductor {nombre} ya existe")

        if st.session_state.conductores:
            st.subheader("Conductores Registrados")
            for idx, c in enumerate(st.session_state.conductores):
                col1, col2 = st.columns([4, 1])
                with col1: st.write(f"**{c.nombre}** - Cédula: {c.cedula}")
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
                    es_frontera = st.checkbox("¿Es viaje a frontera?", value=ruta_obj.es_frontera)
                    hubo_parqueo = st.checkbox("¿Hubo parqueo?", value=False)
                    hubo_anticipo_empresa = st.checkbox("¿Hubo anticipo empresa?", value=False)
                with col2:
                    flypass = limpiar_numero(st.text_input("Flypass (COP)", value="", placeholder="0"))
                    peajes = limpiar_numero(st.text_input("Peajes (COP)", value="", placeholder="0"))
                with col3:
                    hotel = limpiar_numero(st.text_input("Hotel (COP)", value="", placeholder="0"))
                    comida = limpiar_numero(st.text_input("Comida (COP)", value="", placeholder="0"))
                
                col1, col2, col3 = st.columns(3)
                with col1: cargue_descargue = limpiar_numero(st.text_input("Cargue/Descargue (COP)", value="", placeholder="0"))
                with col2: otros = limpiar_numero(st.text_input("Otros (COP)", value="", placeholder="0"))
                with col3: anticipo = limpiar_numero(st.text_input("Anticipo (COP)", value="", placeholder="0"))

                st.divider()
                st.subheader("💰 Valor del Flete")
                valor_flete = limpiar_numero(st.text_input("💵 Valor del Flete (COP)", value="", placeholder="Ej: 5.000.000"))
                if valor_flete > 0: st.success(f"✅ Flete: ${formatear_numero(valor_flete)} COP")

                if valor_flete > 0:
                    calc_preview = CalculadoraCostos(tractomula_obj, conductor_obj, ruta_obj, dias_viaje, es_frontera, hubo_parqueo, flypass, peajes, hotel, comida, cargue_descargue, otros, valor_flete, anticipo, hubo_anticipo_empresa, datos)
                    costos_preview = calc_preview.calcular_costos_totales()
                    col1, col2, col3, col4 = st.columns(4)
                    with col1: st.metric("Total Gastos", f"${formatear_numero(costos_preview['total_gastos'])}")
                    with col2: st.metric("Utilidad", f"${formatear_numero(costos_preview['utilidad'])}", delta=f"{costos_preview['rentabilidad']:.1f}%")
                    with col3: st.metric("Saldo", f"${formatear_numero(costos_preview['saldo'])}")
                    with col4: st.metric("Punto Equilibrio", f"${formatear_numero(costos_preview['punto_equilibrio'])}")

                observaciones = st.text_area("Observaciones")
                col_btn1, col_btn2 = st.columns(2)
                with col_btn1: calcular = st.form_submit_button("📊 Calcular Costos", type="primary")
                with col_btn2: guardar = st.form_submit_button("💾 Calcular y Guardar", type="secondary")

                if calcular or guardar:
                    if valor_flete <= 0: st.error("⚠️ Debes ingresar el Valor del Flete")
                    else:
                        calculadora = CalculadoraCostos(tractomula_obj, conductor_obj, ruta_obj, dias_viaje, es_frontera, hubo_parqueo, flypass, peajes, hotel, comida, cargue_descargue, otros, valor_flete, anticipo, hubo_anticipo_empresa, datos)
                        st.session_state.calculadoras.append(calculadora)
                        if guardar:
                            viaje_id = db.guardar_viaje(calculadora, observaciones)
                            costos = calculadora.calcular_costos_totales()
                            utilidad = costos.get('utilidad', 0)
                            if utilidad >= 0:
                                st.success(f"✅ **Viaje guardado (ID: {viaje_id})**<br>Utilidad: ${formatear_numero(utilidad)}")
                            else:
                                st.error(f"⚠️ **Pérdida detectada (ID: {viaje_id})**<br>Pérdida: ${formatear_numero(utilidad)}")
                        else:
                            st.success("✅ Cálculo completado! Ve a la pestaña de Reportes.")

    with tab5:
        st.header("📄 Reportes y Descargas")
        if st.session_state.calculadoras:
            for idx, calc in enumerate(st.session_state.calculadoras, 1):
                st.subheader(f"Reporte {idx}: {calc.ruta.origen} → {calc.ruta.destino}")
                st.text(GeneradorReportes.generar_reporte_texto(calc))
            excel_data = GeneradorReportes.generar_excel(st.session_state.calculadoras)
            st.download_button("📥 Descargar Excel", data=excel_data, file_name=f"Reporte_{datetime.now().strftime('%d-%m-%Y')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            if st.button("🗑️ Limpiar reportes"): st.session_state.calculadoras = []; st.rerun()
        else: st.info("Realiza un cálculo primero.")

    with tab6:
        st.header("📂 Trazabilidad de Viajes")
        with st.expander("🔍 Filtros", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                fecha_inicio = st.date_input("Fecha desde")
                placa_filtro = st.selectbox("Placa", ["Todas"] + sorted(PLACA_CONDUCTOR.keys()))
            with col2:
                fecha_fin = st.date_input("Fecha hasta")
                conductor_filtro = st.text_input("Conductor")
            with col3:
                origen_filtro = st.text_input("Origen")
                destino_filtro = st.text_input("Destino")
            buscar = st.button("🔍 Buscar", type="primary")

        if buscar or 'ultima_busqueda' not in st.session_state:
            params = []
            query = "SELECT * FROM viajes WHERE 1=1"
            if fecha_inicio: query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') >= %s"; params.append(fecha_inicio.strftime('%Y-%m-%d'))
            if fecha_fin: query += " AND to_date(fecha_creacion, 'YYYY-MM-DD') <= %s"; params.append(fecha_fin.strftime('%Y-%m-%d'))
            if placa_filtro != "Todas": query += " AND placa = %s"; params.append(placa_filtro)
            if conductor_filtro: query += " AND conductor LIKE %s"; params.append(f"%{conductor_filtro}%")
            if origen_filtro: query += " AND origen LIKE %s"; params.append(f"%{origen_filtro}%")
            if destino_filtro: query += " AND destino LIKE %s"; params.append(f"%{destino_filtro}%")
            query += " ORDER BY fecha_creacion DESC"
            df_viajes = db.buscar_viajes(fecha_inicio.strftime('%Y-%m-%d') if fecha_inicio else None, fecha_fin.strftime('%Y-%m-%d') if fecha_fin else None, placa_filtro if placa_filtro != "Todas" else None, conductor_filtro, origen_filtro, destino_filtro)
            st.session_state.ultima_busqueda = df_viajes
        else: df_viajes = st.session_state.ultima_busqueda

        if not df_viajes.empty:
            st.success(f"Se encontraron {len(df_viajes)} viajes")
            col1, col2, col3, col4 = st.columns(4)
            with col1: st.metric("Total Viajes", len(df_viajes))
            with col2: st.metric("Kilómetros", f"{formatear_numero(df_viajes['distancia_km'].sum())} km")
            with col3: st.metric("Gastos", f"${formatear_numero(df_viajes['total_gastos'].sum())}")
            with col4: st.metric("Utilidad", f"${formatear_numero(df_viajes['utilidad'].sum())}")
            
            cols_to_show = ['id', 'fecha_creacion', 'placa', 'conductor', 'origen', 'destino', 'distancia_km', 'total_gastos', 'valor_flete', 'utilidad']
            df_mostrar = df_viajes[cols_to_show].copy()
            df_mostrar.columns = ['ID', 'Fecha', 'Placa', 'Conductor', 'Origen', 'Destino', 'Km', 'Gastos', 'Flete', 'Utilidad']
            df_mostrar['Gastos'] = df_mostrar['Gastos'].apply(lambda x: f"${formatear_numero(x)}")
            df_mostrar['Flete'] = df_mostrar['Flete'].apply(lambda x: f"${formatear_numero(x)}")
            df_mostrar['Utilidad'] = df_mostrar['Utilidad'].apply(lambda x: f"${formatear_numero(x)}")
            st.dataframe(df_mostrar, use_container_width=True)

            viaje_id_seleccionado = st.selectbox("Ver detalle", df_viajes['id'].tolist())
            if st.button("Ver Detalle"):
                viaje = db.obtener_viaje_por_id(viaje_id_seleccionado)
                if viaje:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("### Info")
                        st.write(f"ID: {viaje[0]}, Fecha: {viaje[1]}, Placa: {viaje[2]}")
                        st.write(f"Ruta: {viaje[4]} → {viaje[5]}, Km: {formatear_numero(viaje[6])}")
                        st.write(f"Conductor: {viaje[3]}")
                    with col2:
                        st.markdown("### Resultados")
                        st.write(f"Gastos: ${formatear_numero(viaje[28])}, Flete: ${formatear_numero(viaje[31])}")
                        utilidad = viaje[32] if viaje[32] is not None else 0
                        if utilidad >= 0: st.success(f"Utilidad: ${formatear_numero(utilidad)}")
                        else: st.error(f"Pérdida: ${formatear_numero(utilidad)}")
                        st.write(f"Saldo: ${formatear_numero(viaje[35])}")
                    if st.button("Eliminar", key="del_viaje"):
                        db.eliminar_viaje(viaje_id_seleccionado)
                        st.success("Eliminado"); st.rerun()

    with tab7:
        st.header("Acumulado por Flota")
        df_totales = db.obtener_totales_por_placa()
        if not df_totales.empty:
            st.dataframe(df_totales, use_container_width=True)
            fig = px.bar(df_totales, x='placa', y='total_ut')
            st.plotly_chart(fig)

if __name__ == "__main__":
    main()
