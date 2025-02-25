# -*- coding: utf-8 -*-
"""
Scraper Empresite - Modo Debug
Autor: [Jhon Limones]
Fecha: 24/02/2025
"""

import pandas as pd
from typing import Dict
from pathlib import Path
import logging

# Importar las clases del scraper principal
from prueba_py import (
    ConfiguracionScraper,
    ProcesadorEmpresas,
    logger,
    crear_estructura_directorios,
    COLUMNAS_SALIDA,  # Importar las columnas para asegurar que se incluyan en la salida
    guardar_resultados_ordenados  # Importar la nueva función que añadiremos a prueba_py.py
)

def ejecutar_debug(razon_social: str = None):
    """Ejecuta el scraper en modo debug con una sola empresa."""
    try:
        # Configurar el scraper en modo depuración
        config = ConfiguracionScraper()
        config.modo_headless = False  # Asegurar que el navegador sea visible para depuración
        config.procesos = 1  # Solo un proceso en modo debug
        config.chunk_size = 1  # Una empresa por lote

        print("\n[DEBUG] Creando estructura de directorios...")
        crear_estructura_directorios()

        # Si no se proporciona razón social, leer la primera empresa del archivo CSV
        if not razon_social:
            try:
                df = pd.read_csv(config.archivo_entrada)
                if len(df) > 0:
                    razon_social = df.iloc[0]['RAZON_SOCIAL']
                    print(f"\n[DEBUG] Usando primera empresa del archivo: {razon_social}")
                else:
                    raise ValueError("El archivo de entrada está vacío.")
            except Exception as e:
                print(f"[ERROR] No se pudo leer el archivo de entrada: {str(e)}")
                return
        
        # Cargar los datos originales del CSV para esta empresa
        try:
            df = pd.read_csv(config.archivo_entrada)
            empresa_original = df[df['RAZON_SOCIAL'] == razon_social]
            
            if not empresa_original.empty:
                # Usar los datos originales de la empresa
                empresa = empresa_original.iloc[0].to_dict()
                print(f"[DEBUG] Datos originales cargados del CSV: {list(empresa.keys())}")
            else:
                # Si no se encuentra, inicializar con columnas vacías
                empresa = {col: "" for col in COLUMNAS_SALIDA}
                empresa['RAZON_SOCIAL'] = razon_social  # Asignar al menos el nombre
                print("[DEBUG] No se encontraron datos originales, inicializando vacío")
        except Exception as e:
            print(f"[ERROR] Error al cargar datos originales: {str(e)}. Inicializando vacío.")
            empresa = {col: "" for col in COLUMNAS_SALIDA}
            empresa['RAZON_SOCIAL'] = razon_social  # Asignar el nombre de la empresa
        
        # Crear procesador de empresas
        procesador = ProcesadorEmpresas(config)

        print(f"\n[DEBUG] Iniciando procesamiento de empresa: {razon_social}")
        print("=" * 60)

        # Procesar y obtener resultados
        resultado = procesador.procesar_empresa(empresa)

        # Mostrar resultados en consola
        print("\n[DEBUG] Resultados obtenidos:")
        print("-" * 30)
        for campo, valor in resultado.items():
            if valor:  # Solo mostrar campos con datos
                print(f"{campo}: {valor}")

        # Guardar resultado usando la nueva función de ordenamiento
        archivo_salida = "debug_resultado.csv"
        guardar_resultados_ordenados(resultado, archivo_salida, config.archivo_entrada)
        print(f"\n[DEBUG] Resultados guardados en: {archivo_salida}")

    except Exception as e:
        print(f"\n[ERROR] Ocurrió un fallo en el procesamiento: {str(e)}")
        logger.error(f"Error en modo debug: {str(e)}")
        raise

def main_debug():
    """Función principal para debugging."""
    print("""
╔══════════════════════════════════════════╗
║     Scraper Empresite - Modo Debug       ║
║      Fecha: 24/02/2025                   ║
╚══════════════════════════════════════════╝
    """)
    
    # Ejecutar en modo debug con la primera empresa
    ejecutar_debug()

if __name__ == "__main__":
    main_debug()