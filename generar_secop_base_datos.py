
from __future__ import annotations

import re
import sys
import zipfile
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import fitz  # PyMuPDF
import pandas as pd


# =========================
# Utilidades de extracción
# =========================

def limpiar_texto(texto: str) -> str:
    if not texto:
        return ""
    texto = texto.replace("\x00", " ")
    texto = re.sub(r"\r", "\n", texto)
    texto = re.sub(r"[ \t]+", " ", texto)
    texto = re.sub(r"\n{2,}", "\n", texto)
    return texto.strip()


def leer_pdf_desde_bytes(pdf_bytes: bytes) -> str:
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        doc = fitz.open(tmp_path)
        texto = "\n".join(page.get_text("text") for page in doc)
        return limpiar_texto(texto)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass


def extraer_con_regex(texto: str, patrones: List[str], flags=re.IGNORECASE | re.DOTALL) -> str:
    for patron in patrones:
        m = re.search(patron, texto, flags)
        if m:
            valor = m.group(1).strip()
            valor = re.sub(r"\s+", " ", valor)
            return valor
    return ""


def extraer_campo_bloque(texto: str, etiqueta: str, siguientes_etiquetas: List[str]) -> str:
    """
    Extrae el texto que aparece después de una etiqueta y antes de la siguiente etiqueta conocida.
    """
    todas = [re.escape(x) for x in siguientes_etiquetas]
    patron_fin = "|".join(todas) if todas else r"$"
    patron = rf"{re.escape(etiqueta)}\s*(.*?)(?=\n(?:{patron_fin})\b|$)"
    m = re.search(patron, texto, re.IGNORECASE | re.DOTALL)
    if m:
        valor = m.group(1).strip()
        valor = re.sub(r"\s+", " ", valor)
        return valor
    return ""


def normalizar_moneda(valor: str) -> Optional[float]:
    if not valor:
        return None

    txt = valor.upper().replace("COP", "").replace("$", "").strip()
    txt = txt.replace(" ", "")

    # Caso 1: 34.430.000,00
    if re.match(r"^\d{1,3}(\.\d{3})*(,\d+)?$", txt):
        txt = txt.replace(".", "").replace(",", ".")
    # Caso 2: 34,430,000.00
    elif re.match(r"^\d{1,3}(,\d{3})*(\.\d+)?$", txt):
        txt = txt.replace(",", "")
    else:
        # limpieza general
        txt = re.sub(r"[^0-9,.\-]", "", txt)
        if txt.count(",") == 1 and txt.count(".") > 1:
            txt = txt.replace(".", "").replace(",", ".")
        elif txt.count(",") > 1 and txt.count(".") == 1:
            txt = txt.replace(",", "")
        elif txt.count(",") == 1 and txt.count(".") == 0:
            txt = txt.replace(",", ".")

    try:
        return float(txt)
    except Exception:
        return None


def extraer_fecha_simple(valor: str) -> str:
    if not valor:
        return ""
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", valor)
    return m.group(1) if m else valor.strip()


# =========================
# Extracción por documento
# =========================

ETIQUETAS_ORDENADAS = [
    "Número del proceso",
    "Título:",
    "Fase:",
    "Estado:",
    "Descripción:",
    "Tipo de proceso",
    "Tipo de contrato",
    "Justificación de la modalidad de contratación",
    "Duración del contrato:",
    "Fecha de terminación del contrato:",
    "Dirección de ejecución del contrato",
    "Código UNSPSC",
    "Lista adicional de códigos UNSPSC",
    "Lotes?",
    "¿Es una adquisición del PAA?",
    "PAA",
    "Misión y visión:",
    "Valor total estimado de adquisiciones:",
    "Información",
    "Adquisiciones planeadas",
    "Cronograma",
    "Configuración financiera",
    "Documentación",
    "Información de la selección",
    "Información presupuestal",
    "Cuestionario",
    "Observaciones y Mensajes",
]


def extraer_registro(texto: str, nombre_archivo: str) -> Dict[str, object]:
    registro: Dict[str, object] = {
        "archivo_pdf": nombre_archivo,
        "numero_proceso": "",
        "titulo": "",
        "fase": "",
        "estado": "",
        "descripcion": "",
        "tipo_proceso": "",
        "tipo_contrato": "",
        "justificacion_modalidad": "",
        "duracion_contrato": "",
        "fecha_terminacion_contrato": "",
        "direccion_ejecucion": "",
        "codigo_unspsc": "",
        "descripcion_unspsc": "",
        "lista_codigos_unspsc": "",
        "lotes": "",
        "es_adquisicion_paa": "",
        "paa": "",
        "mision_vision": "",
        "valor_total_estimado_adquisiciones_texto": "",
        "valor_total_estimado_adquisiciones": None,
        "entidad": "",
        "precio_estimado_total_texto": "",
        "precio_estimado_total": None,
        "valor_contrato_texto": "",
        "valor_contrato": None,
        "fecha_publicacion_proceso": "",
        "fecha_inicio_ejecucion": "",
        "plazo_ejecucion_contrato": "",
        "documentos_tipo": "",
        "fuente_recursos": "",
        "anio_proceso": "",
        "texto_extraido": texto[:4000],  # muestra corta para trazabilidad
    }

    lineas = [ln.strip() for ln in texto.splitlines() if ln.strip()]

    # Entidad: suele ir después de "INFORMACIÓN DEL PROCEDIMIENTO" y antes del primer valor monetario
    entidad = extraer_con_regex(
        texto,
        [
            r"INFORMACIÓN DEL PROCEDIMIENTO\s+([A-ZÁÉÍÓÚÑ0-9 ,.\-]+?)\s+\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*COP",
        ],
    )
    registro["entidad"] = entidad

    # Número del proceso
    registro["numero_proceso"] = extraer_campo_bloque(
        texto,
        "Número del proceso",
        ["Título:", "Fase:", "Estado:", "Descripción:"]
    ) or extraer_con_regex(texto, [r"Número del proceso\s+([A-Z0-9.\-_/]+)"])

    # Título
    registro["titulo"] = extraer_campo_bloque(
        texto,
        "Título:",
        ["Fase:", "Estado:", "Descripción:", "Tipo de proceso"]
    )

    # Fase, estado, descripción
    registro["fase"] = extraer_campo_bloque(texto, "Fase:", ["Estado:", "Descripción:", "Tipo de proceso"])
    registro["estado"] = extraer_campo_bloque(texto, "Estado:", ["Descripción:", "Tipo de proceso", "Tipo de contrato"])
    registro["descripcion"] = extraer_campo_bloque(texto, "Descripción:", ["Tipo de proceso", "Tipo de contrato"])

    # Tipo de proceso
    registro["tipo_proceso"] = extraer_campo_bloque(
        texto,
        "Tipo de proceso",
        ["Tipo de contrato", "Justificación de la modalidad de contratación", "Duración del contrato:"]
    )

    # Tipo de contrato
    registro["tipo_contrato"] = extraer_campo_bloque(
        texto,
        "Tipo de contrato",
        ["Justificación de la modalidad de contratación", "Duración del contrato:", "Fecha de terminación del contrato:"]
    )

    # Justificación modalidad
    registro["justificacion_modalidad"] = extraer_campo_bloque(
        texto,
        "Justificación de la modalidad de contratación",
        ["Duración del contrato:", "Fecha de terminación del contrato:", "Dirección de ejecución del contrato"]
    )

    # Duración y fechas
    registro["duracion_contrato"] = extraer_campo_bloque(
        texto,
        "Duración del contrato:",
        ["Fecha de terminación del contrato:", "Dirección de ejecución del contrato", "Código UNSPSC"]
    )
    registro["fecha_terminacion_contrato"] = extraer_fecha_simple(
        extraer_campo_bloque(
            texto,
            "Fecha de terminación del contrato:",
            ["Dirección de ejecución del contrato", "Código UNSPSC", "Lista adicional de códigos UNSPSC"]
        )
    )

    # Dirección
    registro["direccion_ejecucion"] = extraer_campo_bloque(
        texto,
        "Dirección de ejecución del contrato",
        ["Código UNSPSC", "Lista adicional de códigos UNSPSC", "Lotes?"]
    )

    # UNSPSC
    registro["codigo_unspsc"] = extraer_campo_bloque(
        texto,
        "Código UNSPSC",
        ["Lista adicional de códigos UNSPSC", "Lotes?", "¿Es una adquisición del PAA?", "PAA"]
    ) or extraer_con_regex(texto, [r"Código UNSPSC\s+(\d{8})"])

    registro["lista_codigos_unspsc"] = extraer_campo_bloque(
        texto,
        "Lista adicional de códigos UNSPSC",
        ["Lotes?", "¿Es una adquisición del PAA?", "PAA", "Misión y visión:"]
    )

    # Descripción UNSPSC: suele aparecer en sección de adquisiciones planeadas
    registro["descripcion_unspsc"] = extraer_con_regex(
        texto,
        [
            r"Adquisiciones planeadas.*?Código UNSPSC\s+Descripción\s+Tipo\s+Fuente de los recursos\s+Valor total estimado\s+Unidad de contratación\s+.*?\b\d{8}\b\s*-\s*([^\n]+)",
            r"\b" + re.escape(registro["codigo_unspsc"]) + r"\b\s*-\s*([^\n]+)" if registro["codigo_unspsc"] else r"$^",
        ],
    )

    # Lotes / PAA
    registro["lotes"] = extraer_campo_bloque(
        texto,
        "Lotes?",
        ["¿Es una adquisición del PAA?", "PAA", "Misión y visión:"]
    )
    registro["es_adquisicion_paa"] = extraer_campo_bloque(
        texto,
        "¿Es una adquisición del PAA?",
        ["PAA", "Misión y visión:", "Valor total estimado de adquisiciones:"]
    )
    registro["paa"] = extraer_campo_bloque(
        texto,
        "PAA",
        ["Misión y visión:", "Valor total estimado de adquisiciones:", "Adquisiciones planeadas"]
    )

    # Misión y visión
    registro["mision_vision"] = extraer_campo_bloque(
        texto,
        "Misión y visión:",
        ["Valor total estimado de adquisiciones:", "Adquisiciones planeadas", "Código UNSPSC"]
    )

    # Valores
    precio_estimado = extraer_con_regex(
        texto,
        [
            r"Precio estimado total:\s*.*?Información\s+.*?\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*COP)",
            r"Precio estimado total:\s*([0-9.,]+\s*COP)",
        ],
    )
    registro["precio_estimado_total_texto"] = precio_estimado
    registro["precio_estimado_total"] = normalizar_moneda(precio_estimado)

    vtea = extraer_campo_bloque(
        texto,
        "Valor total estimado de adquisiciones:",
        ["Adquisiciones planeadas", "Código UNSPSC", "Descripción", "Tipo"]
    ) or extraer_con_regex(
        texto,
        [r"Valor total estimado de adquisiciones:\s*([0-9.,]+\s*COP)"]
    )
    registro["valor_total_estimado_adquisiciones_texto"] = vtea
    registro["valor_total_estimado_adquisiciones"] = normalizar_moneda(vtea)

    valor_contrato = extraer_con_regex(
        texto,
        [
            r"Información de la selección.*?Valor del contrato\s+.*?\s+(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d+)?\s*COP)",
            r"VALOR DEL CONTRATO.*?\b(\d{1,3}(?:\.\d{3})*(?:,\d+)?)(?!\s*UNSPSC)",
        ],
    )
    registro["valor_contrato_texto"] = valor_contrato
    registro["valor_contrato"] = normalizar_moneda(valor_contrato)

    # Fuente de recursos
    registro["fuente_recursos"] = extraer_con_regex(
        texto,
        [
            r"Fuente de los recursos:\s*(.+?)\s+Valor",
            r"Fuente de los recursos\s+([^\n]+)",
        ],
    )

    # Cronograma / fechas
    fechas = re.findall(r"\d{1,2}/\d{1,2}/\d{4}", texto)
    if fechas:
        registro["fecha_publicacion_proceso"] = fechas[0]
        if len(fechas) >= 2:
            registro["fecha_inicio_ejecucion"] = fechas[1]
        if len(fechas) >= 3 and not registro["fecha_terminacion_contrato"]:
            registro["fecha_terminacion_contrato"] = fechas[2]

    plazo = extraer_con_regex(
        texto,
        [
            r"Plazo de ejecución del contrato\s+([^\n]+)",
            r"Duración del contrato:\s+([^\n]+)",
        ],
    )
    registro["plazo_ejecucion_contrato"] = plazo

    # Documentos tipo
    registro["documentos_tipo"] = extraer_con_regex(
        texto,
        [
            r"Documentos Tipo\s+([^\n]+)",
        ],
    )

    # Año del proceso
    anio = ""
    if registro["numero_proceso"]:
        m_anio = re.search(r"(20\d{2})", registro["numero_proceso"])
        if m_anio:
            anio = m_anio.group(1)
    if not anio and registro["fecha_publicacion_proceso"]:
        m_anio2 = re.search(r"(20\d{2})", registro["fecha_publicacion_proceso"])
        if m_anio2:
            anio = m_anio2.group(1)
    registro["anio_proceso"] = anio

    # Limpieza final de sí/no cortos
    for campo in ["lotes", "es_adquisicion_paa", "paa"]:
        if isinstance(registro[campo], str):
            valor = registro[campo].strip()
            if "Sí" in valor and "No" in valor:
                registro[campo] = "Sí/No"
            else:
                registro[campo] = valor

    return registro


# =========================
# Procesamiento principal
# =========================

def procesar_zip(zip_path: Path) -> pd.DataFrame:
    registros = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        pdfs = [n for n in zf.namelist() if n.lower().endswith(".pdf")]

        for i, pdf_name in enumerate(pdfs, start=1):
            try:
                pdf_bytes = zf.read(pdf_name)
                texto = leer_pdf_desde_bytes(pdf_bytes)
                registro = extraer_registro(texto, Path(pdf_name).name)
                registros.append(registro)
                print(f"[{i}/{len(pdfs)}] OK - {Path(pdf_name).name}")
            except Exception as e:
                registros.append({
                    "archivo_pdf": Path(pdf_name).name,
                    "error_extraccion": str(e),
                })
                print(f"[{i}/{len(pdfs)}] ERROR - {Path(pdf_name).name}: {e}")

    df = pd.DataFrame(registros)

    columnas_base = [
        "archivo_pdf",
        "numero_proceso",
        "titulo",
        "entidad",
        "tipo_proceso",
        "tipo_contrato",
        "justificacion_modalidad",
        "estado",
        "fase",
        "descripcion",
        "codigo_unspsc",
        "descripcion_unspsc",
        "lista_codigos_unspsc",
        "fuente_recursos",
        "duracion_contrato",
        "plazo_ejecucion_contrato",
        "fecha_publicacion_proceso",
        "fecha_inicio_ejecucion",
        "fecha_terminacion_contrato",
        "direccion_ejecucion",
        "lotes",
        "es_adquisicion_paa",
        "paa",
        "mision_vision",
        "precio_estimado_total_texto",
        "precio_estimado_total",
        "valor_total_estimado_adquisiciones_texto",
        "valor_total_estimado_adquisiciones",
        "valor_contrato_texto",
        "valor_contrato",
        "documentos_tipo",
        "anio_proceso",
        "texto_extraido",
        "error_extraccion",
    ]

    for col in columnas_base:
        if col not in df.columns:
            df[col] = None

    return df[columnas_base]


def guardar_excel(df: pd.DataFrame, output_path: Path) -> None:
    resumen = pd.DataFrame({
        "indicador": [
            "total_registros",
            "registros_con_numero_proceso",
            "registros_con_tipo_contrato",
            "registros_con_codigo_unspsc",
            "registros_con_valor_contrato",
            "registros_con_error",
        ],
        "valor": [
            len(df),
            int(df["numero_proceso"].fillna("").astype(str).str.len().gt(0).sum()),
            int(df["tipo_contrato"].fillna("").astype(str).str.len().gt(0).sum()),
            int(df["codigo_unspsc"].fillna("").astype(str).str.len().gt(0).sum()),
            int(df["valor_contrato"].notna().sum()),
            int(df["error_extraccion"].fillna("").astype(str).str.len().gt(0).sum()),
        ],
    })

    diccionario = pd.DataFrame({
        "variable": [
            "archivo_pdf", "numero_proceso", "titulo", "entidad", "tipo_proceso", "tipo_contrato",
            "justificacion_modalidad", "estado", "fase", "descripcion", "codigo_unspsc",
            "descripcion_unspsc", "lista_codigos_unspsc", "fuente_recursos", "duracion_contrato",
            "plazo_ejecucion_contrato", "fecha_publicacion_proceso", "fecha_inicio_ejecucion",
            "fecha_terminacion_contrato", "direccion_ejecucion", "lotes", "es_adquisicion_paa",
            "paa", "mision_vision", "precio_estimado_total", "valor_total_estimado_adquisiciones",
            "valor_contrato", "documentos_tipo", "anio_proceso", "texto_extraido", "error_extraccion"
        ],
        "descripcion": [
            "Nombre del PDF dentro del ZIP",
            "Identificador del proceso SECOP",
            "Título del proceso/contrato",
            "Entidad contratante",
            "Modalidad o tipo de proceso",
            "Tipo de contrato",
            "Justificación de la modalidad de contratación",
            "Estado del proceso",
            "Fase del proceso",
            "Descripción general del proceso",
            "Código principal UNSPSC",
            "Descripción asociada al código UNSPSC",
            "Códigos UNSPSC adicionales",
            "Fuente de los recursos",
            "Duración del contrato en texto",
            "Plazo de ejecución del contrato",
            "Fecha de publicación del proceso",
            "Fecha de inicio de ejecución",
            "Fecha de terminación del contrato",
            "Dirección de ejecución del contrato",
            "Indica si hay lotes",
            "Indica si es adquisición del PAA",
            "Campo PAA",
            "Texto de misión y visión",
            "Precio estimado total en valor numérico",
            "Valor total estimado de adquisiciones en valor numérico",
            "Valor del contrato en valor numérico",
            "Información sobre documentos tipo",
            "Año inferido del proceso",
            "Muestra corta del texto extraído para trazabilidad",
            "Mensaje de error si falló la extracción"
        ]
    })

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        resumen.to_excel(writer, index=False, sheet_name="Resumen")
        df.to_excel(writer, index=False, sheet_name="Base_Datos")
        diccionario.to_excel(writer, index=False, sheet_name="Diccionario")


def main():
    if len(sys.argv) < 2:
        print("Uso: python generar_secop_base_datos.py /ruta/al/archivo.zip")
        sys.exit(1)

    zip_path = Path(sys.argv[1])

    if not zip_path.exists():
        print(f"No existe el archivo: {zip_path}")
        sys.exit(1)

    output_path = zip_path.parent / "secop_base_datos.xlsx"

    df = procesar_zip(zip_path)
    guardar_excel(df, output_path)

    csv_path = zip_path.parent / "secop_base_datos.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ Archivo Excel generado en: {output_path}")
    print(f"✅ Archivo CSV generado en:   {csv_path}")


if __name__ == "__main__":
    main()
