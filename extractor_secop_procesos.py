import io
import os
import re
import sys
import zipfile
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


# =========================================================
# CONFIGURACIÓN
# =========================================================
INPUT_ZIP_PATH = r""
OUTPUT_EXCEL_FILENAME = "secop_procedimiento_extraidos.xlsx"
USE_AI = False
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

OUTPUT_COLUMNS = [
    "archivo_pdf",
    "id_publicacion",
    "url",
    "entidad",
    "precio_estimado_total",
    "numero_proceso",
    "titulo",
    "fase",
    "estado",
    "descripcion",
    "tipo_proceso",
    "tipo_contrato",
    "justificacion_modalidad",
    "duracion_contrato",
    "fecha_terminacion_contrato",
    "direccion_ejecucion_contrato",
    "codigo_unspsc",
    "descripcion_unspsc",
    "fecha_publicacion_proceso",
    "entidad_adjudicataria",
    "valor_contrato",
    "destinacion_gasto",
    "paa_anio",
    "CDP_Código",
    "CDP_Tipo",
    "CDP_Estado",
    "CDP_Saldo",
    "CDP_Valor a utilizar",
    "CDP_Código unidad/subunidad ejecutora",
    "precio_estimado_total_cop",
    "valor_contrato_cop",
    "anio_proceso",
    "texto_extraido_chars",
    "extraccion_ok",
]

KNOWN_FASES = [
    "Presentación de oferta",
    "Presentacion de oferta",
]
KNOWN_ESTADOS = [
    "Proceso adjudicado y celebrado",
    "Proceso adjudicado",
    "Proceso celebrado",
]
PROCESS_TYPE_MARKERS = [
    "Contratación directa",
    "Contratacion directa",
    "Contratación régimen especial",
    "Contratacion regimen especial",
    "Contratación régimen especial (con ofertas)",
    "Contratacion regimen especial (con ofertas)",
    "Contratación directa (con ofertas)",
    "Contratacion directa (con ofertas)",
    "Mínima cuantía",
    "Minima cuantia",
    "Selección abreviada",
    "Seleccion abreviada",
    "Licitación pública",
    "Licitacion publica",
    "Concurso de méritos",
    "Concurso de meritos",
    "Subasta inversa",
]

CDP_HEADER_LABELS = {
    "Código",
    "Tipo",
    "Estado",
    "Saldo",
    "Valor a utilizar",
    "Código unidad/subunidad ejecutora",
}
CDP_STOP_MARKERS = [
    "Saldo de CDP",
    "Saldo de vigencias futuras",
    "Saldo total a comprometer",
    "Última consulta a SIIF",
    "Fecha de consulta SIIF",
    "Información de la selección",
    "Visita al lugar de ejecución",
    "Cuestionario",
    "Configuración financiera",
]
CDP_SECTION_PATTERN = r"CDP/Vigencias Futuras"


# =========================================================
# UTILIDADES
# =========================================================
def normalize_spaces(text: str) -> str:
    text = text or ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    cleaned = []
    for ch in t:
        cat = unicodedata.category(ch)
        if cat.startswith("C") and ch not in ["\n", "\t"]:
            continue
        cleaned.append(ch)
    return normalize_spaces("".join(cleaned))


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text or "") if unicodedata.category(c) != "Mn")


def safe_join(lines: List[str]) -> str:
    return normalize_spaces(" ".join([x.strip() for x in lines if x and x.strip()]))


def non_empty_lines(text: str) -> List[str]:
    return [ln.strip() for ln in text.splitlines() if ln.strip()]


def parse_money(text: str) -> Optional[float]:
    if not text:
        return None
    s = str(text).upper().replace("COP", "").replace("$", "").replace(" ", "")
    if "," in s and "." in s and s.rfind(",") > s.rfind("."):
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(".", "").replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def first_index(lines: List[str], predicate, start: int = 0) -> int:
    for i in range(start, len(lines)):
        if predicate(lines[i]):
            return i
    return -1


def find_line(lines: List[str], pattern: str, start: int = 0, flags=re.IGNORECASE) -> int:
    reg = re.compile(pattern, flags)
    return first_index(lines, lambda x: bool(reg.search(x)), start)


def is_money_line(line: str) -> bool:
    return bool(re.search(r"\b\d[\d\.\,]*\s*COP\b", line, re.IGNORECASE))


def is_datetime_line(line: str) -> bool:
    return bool(re.search(r"\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+(?:AM|PM)", line, re.IGNORECASE))


def is_year_line(line: str) -> bool:
    return bool(re.fullmatch(r"20\d{2}", line.strip()))


def is_unspsc_line(line: str) -> bool:
    return bool(re.match(r"^\d{8}\s*-\s*.+", line))


def starts_process_type(line: str) -> bool:
    plain = strip_accents(line).lower()
    markers = [strip_accents(x).lower() for x in PROCESS_TYPE_MARKERS]
    return any(plain.startswith(m) for m in markers)


def clean_entity_name(value: str) -> str:
    value = normalize_spaces(value)
    value = re.sub(r"\b(Descargar|Ver contrato|Documento\(s\)|Evaluación|Evaluacion)\b.*$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\b(¿Permitir visitas al lugar de ejecución\?|Visita al lugar de ejecución|Proyecto del Plan Marco|Destinación del gasto|Destinacion del gasto)\b.*$", "", value, flags=re.IGNORECASE)
    return value.strip(" -")


def get_id_publicacion(filename: str) -> str:
    stem = Path(filename).stem
    m = re.search(r"(CO1\.[A-Z]+\.\d+)", stem, re.IGNORECASE)
    return m.group(1) if m else stem


def build_url_from_id_publicacion(id_publicacion: str) -> str:
    id_publicacion = normalize_spaces(id_publicacion)
    if not id_publicacion:
        return ""
    return (
        "https://community.secop.gov.co/Public/Tendering/OpportunityDetail/Index"
        f"?noticeUID={id_publicacion}&isFromPublicArea=True&isModal=true&asPopupView=true"
    )


def extract_numero_proceso(text: str, filename: str = "") -> str:
    patterns = [
        r"\b(ATENEA\s*-\s*[A-Z0-9]+\s*-\s*20\d{2}(?:\s+copia)?)\b",
        r"\b(INV\s*-\s*\d+\s*-\s*\d+)\b",
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return normalize_spaces(m.group(1)).replace(" - ", "-")
    m2 = re.search(r"(CO1\.[A-Z]+\.\d+)", filename, re.IGNORECASE)
    return m2.group(1) if m2 else ""


def extract_anio_proceso(numero_proceso: str, fecha_publicacion: str) -> Optional[int]:
    m = re.search(r"(20\d{2})", numero_proceso or "")
    if m:
        return int(m.group(1))
    m = re.search(r"/(20\d{2})\s", fecha_publicacion or "")
    if m:
        return int(m.group(1))
    return None


def is_cdp_stop_line(line: str) -> bool:
    plain = normalize_spaces(strip_accents(line)).lower()
    for marker in CDP_STOP_MARKERS:
        if plain.startswith(normalize_spaces(strip_accents(marker)).lower()):
            return True
    return False


def parse_cdp_fields(lines: List[str], text: str) -> Dict[str, str]:
    """
    Extrae la primera fila de la tabla ubicada en la sección 'CDP/Vigencias Futuras'.

    Regla observada en los PDFs de muestra:
    - después del título de la sección, aparecen los 6 valores de la fila en este orden:
      Código, Tipo, Estado, Saldo, Valor a utilizar, Código unidad/subunidad ejecutora
    - más adelante vuelven a aparecer los encabezados de la tabla
    - la sección termina antes de marcadores como 'Saldo de CDP', 'Información de la selección', etc.
    """
    out = {
        "CDP_Código": "",
        "CDP_Tipo": "",
        "CDP_Estado": "",
        "CDP_Saldo": "",
        "CDP_Valor a utilizar": "",
        "CDP_Código unidad/subunidad ejecutora": "",
    }

    idx = find_line(lines, CDP_SECTION_PATTERN)
    if idx == -1:
        return out

    values = []
    i = idx + 1
    while i < len(lines):
        line = normalize_spaces(lines[i])
        if not line:
            i += 1
            continue

        if is_cdp_stop_line(line):
            break

        if line in CDP_HEADER_LABELS:
            i += 1
            continue

        values.append(line)
        if len(values) >= 6:
            break
        i += 1

    if len(values) >= 6:
        out["CDP_Código"] = values[0]
        out["CDP_Tipo"] = values[1]
        out["CDP_Estado"] = values[2]
        out["CDP_Saldo"] = values[3]
        out["CDP_Valor a utilizar"] = values[4]
        out["CDP_Código unidad/subunidad ejecutora"] = values[5]
        return out

    # Fallback por regex sobre texto completo si la lectura por líneas no alcanza
    pattern = re.compile(
        r"CDP/Vigencias Futuras.*?\n"
        r"([^\n]+)\n"
        r"([^\n]+)\n"
        r"([^\n]+)\n"
        r"([^\n]+COP)\n"
        r"([^\n]+COP)\n"
        r"([^\n]+?)\n"
        r"(?:Saldo de CDP|Saldo de vigencias futuras|Información de la selección)",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(text)
    if m:
        out["CDP_Código"] = normalize_spaces(m.group(1))
        out["CDP_Tipo"] = normalize_spaces(m.group(2))
        out["CDP_Estado"] = normalize_spaces(m.group(3))
        out["CDP_Saldo"] = normalize_spaces(m.group(4))
        out["CDP_Valor a utilizar"] = normalize_spaces(m.group(5))
        out["CDP_Código unidad/subunidad ejecutora"] = normalize_spaces(m.group(6))

    return out


# =========================================================
# PDF
# =========================================================
def extract_text_with_pymupdf(pdf_bytes: bytes) -> str:
    if fitz is None:
        return ""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        return "\n".join((page.get_text("text") or "") for page in doc)
    except Exception:
        return ""


def extract_text_with_pypdf(pdf_bytes: bytes) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                pages.append("")
        return "\n".join(pages)
    except Exception:
        return ""


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    text = extract_text_with_pymupdf(pdf_bytes)
    if len(normalize_text(text)) >= 200:
        return text
    alt = extract_text_with_pypdf(pdf_bytes)
    if len(normalize_text(alt)) > len(normalize_text(text)):
        return alt
    return text


# =========================================================
# REGLAS ESPECIALIZADAS PARA PDF SECOP DE PROCESO
# =========================================================
def parse_procedure_fields(lines: List[str]) -> Dict[str, str]:
    out = {
        "entidad": "",
        "precio_estimado_total": "",
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
        "direccion_ejecucion_contrato": "",
        "codigo_unspsc": "",
        "descripcion_unspsc": "",
        "fecha_publicacion_proceso": "",
        "entidad_adjudicataria": "",
        "valor_contrato": "",
        "destinacion_gasto": "",
        "paa_anio": "",
    }

    info_idx = find_line(lines, r"^INFORMACI[ÓO]N DEL PROCEDIMIENTO$")
    if info_idx == -1:
        return out

    start = info_idx + 1
    if start < len(lines) and strip_accents(lines[start]).lower() == "informacion":
        start += 1

    money_idx = first_index(lines, is_money_line, start)
    if money_idx != -1:
        out["entidad"] = safe_join(lines[start:money_idx])
        out["precio_estimado_total"] = lines[money_idx]

        numero_idx = first_index(
            lines,
            lambda x: bool(re.search(r"ATENEA\s*-\s*[A-Z0-9]+\s*-\s*20\d{2}(?:\s+copia)?", x, re.IGNORECASE)),
            money_idx + 1,
        )
        if numero_idx != -1:
            out["numero_proceso"] = normalize_spaces(lines[numero_idx]).replace(" - ", "-")

            fase_idx = first_index(lines, lambda x: x in KNOWN_FASES, numero_idx + 1)
            if fase_idx != -1:
                out["titulo"] = safe_join(lines[numero_idx + 1:fase_idx])
                out["fase"] = lines[fase_idx]

                estado_idx = fase_idx + 1 if fase_idx + 1 < len(lines) else -1
                if estado_idx != -1:
                    out["estado"] = lines[estado_idx]

                    datos_idx = find_line(lines, r"^Datos del contrato$", estado_idx + 1)
                    if datos_idx != -1:
                        chunk = lines[estado_idx + 1:datos_idx]
                        proc_idx = first_index(chunk, starts_process_type, 0)
                        if proc_idx != -1:
                            out["descripcion"] = safe_join(chunk[:proc_idx])
                            out["tipo_proceso"] = safe_join(chunk[proc_idx:])
                        else:
                            out["descripcion"] = safe_join(chunk)

                        i = datos_idx + 1

                        if i < len(lines):
                            out["tipo_contrato"] = lines[i]
                            i += 1

                        if i < len(lines):
                            out["justificacion_modalidad"] = lines[i]
                            i += 1

                        if i < len(lines):
                            out["duracion_contrato"] = lines[i]
                            i += 1

                        if i < len(lines) and is_datetime_line(lines[i]):
                            out["fecha_terminacion_contrato"] = lines[i]
                            i += 1

                        address_lines = []
                        while i < len(lines) and not is_unspsc_line(lines[i]):
                            if lines[i] in {"Sí", "Si", "No"} or strip_accents(lines[i]).lower() == "plan anual de adquisiciones":
                                break
                            address_lines.append(lines[i])
                            i += 1
                        out["direccion_ejecucion_contrato"] = safe_join(address_lines)

                        if i < len(lines) and is_unspsc_line(lines[i]):
                            m_unspsc = re.match(r"^(\d{8})\s*-\s*(.+)$", lines[i])
                            if m_unspsc:
                                out["codigo_unspsc"] = m_unspsc.group(1)
                                out["descripcion_unspsc"] = normalize_spaces(m_unspsc.group(2))

    paa_idx = find_line(lines, r"^Plan anual de adquisiciones$")
    if paa_idx != -1:
        yidx = first_index(lines, is_year_line, paa_idx + 1)
        if yidx != -1:
            out["paa_anio"] = lines[yidx]

    cron_idx = find_line(lines, r"^Cronograma$")
    conf_idx = find_line(lines, r"^Configuración financiera$", cron_idx + 1) if cron_idx != -1 else -1
    if cron_idx != -1:
        end = conf_idx if conf_idx != -1 else min(len(lines), cron_idx + 25)
        dts = [x for x in lines[cron_idx:end] if is_datetime_line(x)]
        if dts:
            out["fecha_publicacion_proceso"] = dts[-1]

    sel_idx = find_line(lines, r"^Información de la selección$")
    if sel_idx != -1:
        cursor = sel_idx + 1
        headers_to_skip = {"Entidad adjudicataria", "Valor del contrato", "Documento(s)", "Evaluación", "Evaluacion"}
        while cursor < len(lines) and lines[cursor] in headers_to_skip:
            cursor += 1

        adjud_lines = []
        while cursor < len(lines):
            line = lines[cursor]
            if is_money_line(line):
                out["valor_contrato"] = line
                break
            if line in {"Descargar", "Ver contrato"}:
                break
            if re.search(r"^(¿Permitir visitas|Visita al lugar de ejecución|Información presupuestal)$", line, re.IGNORECASE):
                break
            adjud_lines.append(line)
            cursor += 1

        out["entidad_adjudicataria"] = clean_entity_name(safe_join(adjud_lines))

        if not out["valor_contrato"]:
            for line in lines[sel_idx: min(len(lines), sel_idx + 12)]:
                if is_money_line(line):
                    out["valor_contrato"] = line
                    break

    for line in lines:
        plain = strip_accents(line).lower()
        if plain == "inversion":
            out["destinacion_gasto"] = "Inversión"
            break
        if plain == "funcionamiento":
            out["destinacion_gasto"] = "Funcionamiento"
            break

    return out


def fallback_extract(text: str, current: Dict[str, str]) -> Dict[str, str]:
    out = dict(current)

    if not out["entidad"]:
        m = re.search(
            r"(AGENCIA DISTRITAL PARA LA\s+EDUCACI[ÓO]N SUPERIOR,\s+LA CIENCIA Y\s+LA TECNOLOG[ÍI]A,?\s+ATENEA)",
            text,
            re.IGNORECASE,
        )
        if m:
            out["entidad"] = normalize_spaces(m.group(1))

    if not out["precio_estimado_total"]:
        m = re.search(r"INFORMACI[ÓO]N DEL PROCEDIMIENTO.*?(\d[\d\.\,]*\s*COP)", text, re.IGNORECASE | re.DOTALL)
        if m:
            out["precio_estimado_total"] = normalize_spaces(m.group(1))

    if not out["numero_proceso"]:
        out["numero_proceso"] = extract_numero_proceso(text)

    if not out["codigo_unspsc"]:
        m = re.search(r"\b(\d{8})\s*-\s*([^\n]+)", text)
        if m:
            out["codigo_unspsc"] = m.group(1)
            out["descripcion_unspsc"] = normalize_spaces(m.group(2))

    if not out["valor_contrato"]:
        m = re.search(r"Información de la selección.*?(\d[\d\.\,]*\s*COP)", text, re.IGNORECASE | re.DOTALL)
        if m:
            out["valor_contrato"] = normalize_spaces(m.group(1))

    return out


# =========================================================
# IA OPCIONAL SOLO COMO APOYO
# =========================================================
def get_openai_client():
    if not USE_AI or not OPENAI_API_KEY or OpenAI is None:
        return None
    try:
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception:
        return None


def ai_fill_missing(client: OpenAI, text: str, row: Dict[str, object]) -> Dict[str, object]:
    missing = [
        k for k in [
            "titulo",
            "descripcion",
            "tipo_proceso",
            "tipo_contrato",
            "justificacion_modalidad",
            "direccion_ejecucion_contrato",
            "entidad_adjudicataria",
        ]
        if not row.get(k)
    ]
    if not missing:
        return row

    prompt = f"""
Devuelve SOLO JSON válido con estos campos exactos:
titulo, descripcion, tipo_proceso, tipo_contrato, justificacion_modalidad, direccion_ejecucion_contrato, entidad_adjudicataria

Reglas:
- No inventes.
- Si no aparece, devuelve "".
- Usa el texto del PDF SECOP tal cual se entienda.
- No incluyas markdown.

Texto:
{text[:14000]}
"""
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Devuelve SOLO JSON válido."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        import json

        raw = resp.choices[0].message.content.strip()
        raw = raw.removeprefix("```json").removesuffix("```").strip()
        data = json.loads(raw)
        for k, v in data.items():
            if k in row and not row.get(k) and v:
                row[k] = str(v).strip()
    except Exception:
        pass
    return row


# =========================================================
# PROCESAMIENTO
# =========================================================
def build_row_from_pdf(pdf_bytes: bytes, filename: str, client=None, use_ai: bool = False) -> Dict[str, object]:
    raw_text = extract_text_from_pdf_bytes(pdf_bytes)
    text = normalize_text(raw_text)
    lines = non_empty_lines(text)

    base = parse_procedure_fields(lines)
    base = fallback_extract(text, base)
    cdp = parse_cdp_fields(lines, text)

    id_publicacion = get_id_publicacion(filename)
    url = build_url_from_id_publicacion(id_publicacion)

    row = {
        "archivo_pdf": Path(filename).name,
        "id_publicacion": id_publicacion,
        "url": url,
        "entidad": base["entidad"],
        "precio_estimado_total": base["precio_estimado_total"],
        "numero_proceso": base["numero_proceso"] or extract_numero_proceso(text, filename),
        "titulo": base["titulo"],
        "fase": base["fase"] or "Presentación de oferta",
        "estado": base["estado"] or "Proceso adjudicado y celebrado",
        "descripcion": base["descripcion"],
        "tipo_proceso": base["tipo_proceso"],
        "tipo_contrato": base["tipo_contrato"],
        "justificacion_modalidad": base["justificacion_modalidad"],
        "duracion_contrato": base["duracion_contrato"],
        "fecha_terminacion_contrato": base["fecha_terminacion_contrato"],
        "direccion_ejecucion_contrato": base["direccion_ejecucion_contrato"],
        "codigo_unspsc": base["codigo_unspsc"],
        "descripcion_unspsc": base["descripcion_unspsc"],
        "fecha_publicacion_proceso": base["fecha_publicacion_proceso"],
        "entidad_adjudicataria": base["entidad_adjudicataria"],
        "valor_contrato": base["valor_contrato"],
        "destinacion_gasto": base["destinacion_gasto"],
        "paa_anio": int(base["paa_anio"]) if str(base["paa_anio"]).isdigit() else None,
        "CDP_Código": cdp["CDP_Código"],
        "CDP_Tipo": cdp["CDP_Tipo"],
        "CDP_Estado": cdp["CDP_Estado"],
        "CDP_Saldo": cdp["CDP_Saldo"],
        "CDP_Valor a utilizar": cdp["CDP_Valor a utilizar"],
        "CDP_Código unidad/subunidad ejecutora": cdp["CDP_Código unidad/subunidad ejecutora"],
        "precio_estimado_total_cop": parse_money(base["precio_estimado_total"]),
        "valor_contrato_cop": parse_money(base["valor_contrato"]),
        "anio_proceso": None,
        "texto_extraido_chars": len(text),
        "extraccion_ok": False,
    }

    if use_ai and client is not None:
        row = ai_fill_missing(client, text, row)

    row["anio_proceso"] = extract_anio_proceso(str(row["numero_proceso"]), str(row["fecha_publicacion_proceso"]))

    required = [
        row["id_publicacion"],
        row["url"],
        row["entidad"],
        row["precio_estimado_total"],
        row["numero_proceso"],
        row["titulo"],
        row["descripcion"],
        row["tipo_proceso"],
        row["tipo_contrato"],
        row["codigo_unspsc"],
        row["valor_contrato"],
    ]
    row["extraccion_ok"] = sum(bool(x) for x in required) >= 9
    return row


def process_zip(zip_path: Path, use_ai: bool = False, client=None) -> List[Dict[str, object]]:
    results = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        pdf_files = [name for name in zf.namelist() if name.lower().endswith(".pdf")]
        if not pdf_files:
            raise ValueError("El ZIP no contiene archivos PDF.")

        total = len(pdf_files)
        for i, name in enumerate(pdf_files, start=1):
            print(f"Procesando {i}/{total}: {Path(name).name}")
            pdf_bytes = zf.read(name)
            try:
                results.append(build_row_from_pdf(pdf_bytes, name, client=client, use_ai=use_ai))
            except Exception:
                id_publicacion = get_id_publicacion(name)
                results.append(
                    {
                        "archivo_pdf": Path(name).name,
                        "id_publicacion": id_publicacion,
                        "url": build_url_from_id_publicacion(id_publicacion),
                        "entidad": "",
                        "precio_estimado_total": "",
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
                        "direccion_ejecucion_contrato": "",
                        "codigo_unspsc": "",
                        "descripcion_unspsc": "",
                        "fecha_publicacion_proceso": "",
                        "entidad_adjudicataria": "",
                        "valor_contrato": "",
                        "destinacion_gasto": "",
                        "paa_anio": None,
                        "CDP_Código": "",
                        "CDP_Tipo": "",
                        "CDP_Estado": "",
                        "CDP_Saldo": "",
                        "CDP_Valor a utilizar": "",
                        "CDP_Código unidad/subunidad ejecutora": "",
                        "precio_estimado_total_cop": None,
                        "valor_contrato_cop": None,
                        "anio_proceso": None,
                        "texto_extraido_chars": 0,
                        "extraccion_ok": False,
                    }
                )
    return results


def save_results(data: List[Dict[str, object]], output_path: Path) -> None:
    df = pd.DataFrame(data)

    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df = df[OUTPUT_COLUMNS]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)


# =========================================================
# INTERFAZ CLI
# =========================================================
def prompt_zip_path(default_path: str = "") -> Path:
    while True:
        print("\nIngresa la ruta completa del archivo .zip que quieres procesar.")
        if default_path:
            print(f"Presiona Enter para usar la ruta por defecto: {default_path}")
        user_input = input("Ruta del ZIP: ").strip().strip('"')
        selected = user_input or default_path
        if not selected:
            print("Debes escribir una ruta.")
            continue
        path = Path(selected)
        if not path.exists():
            print(f"No existe la ruta: {path}")
            continue
        if path.suffix.lower() != ".zip":
            print("El archivo debe ser .zip")
            continue
        return path


def prompt_output_dir(default_dir: str = "") -> Path:
    while True:
        print("\nIngresa la ruta de la carpeta donde deseas guardar el Excel de salida.")
        if default_dir:
            print(f"Presiona Enter para usar la ruta por defecto: {default_dir}")
        user_input = input("Ruta de salida: ").strip().strip('"')
        selected = user_input or default_dir
        if not selected:
            print("Debes escribir una ruta.")
            continue
        out_dir = Path(selected)
        if out_dir.exists() and not out_dir.is_dir():
            print(f"La ruta existe, pero no es una carpeta: {out_dir}")
            continue
        return out_dir


def main():
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        zip_path = Path(sys.argv[1].strip().strip('"'))
    else:
        zip_path = prompt_zip_path(INPUT_ZIP_PATH)

    if len(sys.argv) >= 3 and sys.argv[2].strip():
        user_output = Path(sys.argv[2].strip().strip('"'))
        if user_output.suffix.lower() == ".xlsx":
            output_path = user_output
        else:
            output_path = user_output / OUTPUT_EXCEL_FILENAME
    else:
        output_dir = prompt_output_dir(str(zip_path.parent))
        output_path = output_dir / OUTPUT_EXCEL_FILENAME

    client = get_openai_client()
    data = process_zip(zip_path, use_ai=bool(client), client=client)
    save_results(data, output_path)
    print(f"\nProceso terminado. Excel guardado en: {output_path}")


if __name__ == "__main__":
    main()
