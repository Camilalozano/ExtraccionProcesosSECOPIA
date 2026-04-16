import io
import json
import os
import re
import sys
import unicodedata
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from openai import OpenAI

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


# =========================================================
# CONFIGURACIÓN
# =========================================================
INPUT_ZIP_PATH = r""
OUTPUT_EXCEL_PATH = r"secop_base_datos_robusta.xlsx"
USE_AI = True
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

TARGET_FIELDS = [
    "archivo",
    "tipo_documento_origen",
    "numero_proceso",
    "numero_contrato",
    "tipo_contrato",
    "modalidad",
    "objeto",
    "nombre_contratante",
    "nit_contratante",
    "nombre_contratista",
    "numero_documento_contratista",
    "tipo_documento_contratista",
    "correo_contratista",
    "telefono_contratista",
    "supervisor",
    "fecha_suscripcion",
    "fecha_inicio",
    "fecha_terminacion",
    "plazo_texto",
    "plazo_dias_inferido",
    "valor_contrato_texto",
    "valor_contrato_num",
    "cdp",
    "rp",
    "codigo_unspsc",
    "obligaciones_especificas",
    "fuente_extraccion",
    "error",
]

FORBIDDEN_CONTRACTOR_NUMBERS = {
    "901508361",
    "9015083614",
}
FORBIDDEN_DOC_CONTEXT_TERMS = [
    "AGENCIA ATENEA",
    "ATENEA",
    "CONTRATANTE",
    "LA AGENCIA",
    "NIT 901.508.361",
    "NIT 901508361",
]


# =========================================================
# UTILIDADES
# =========================================================
def safe_json_loads(raw: str) -> dict:
    raw = (raw or "").strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)
    return json.loads(raw)


def normalize_nullable_text(x: Optional[str]) -> str:
    if x is None:
        return ""
    x = str(x).strip()
    x = re.sub(r"[ \t]+", " ", x)
    x = re.sub(r"\n{3,}", "\n\n", x)
    return x.strip()


def only_digits(x: Optional[str]) -> str:
    if not x:
        return ""
    x = str(x).translate(str.maketrans({"O": "0", "o": "0", "I": "1", "l": "1"}))
    return re.sub(r"\D", "", x)


def normalize_spaces(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def limpiar_texto_para_llm(text: str) -> str:
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", text)
    for a, b in [("\u00A0", " "), ("\u200B", ""), ("\u200E", ""), ("\u200F", "")]:
        t = t.replace(a, b)
    cleaned = []
    for ch in t:
        cat = unicodedata.category(ch)
        if cat.startswith("C") and ch not in ["\n", "\t"]:
            continue
        cleaned.append(ch)
    t = "".join(cleaned)
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t.strip()


def normalize_text(text: str) -> str:
    if not text:
        return ""
    return normalize_spaces(limpiar_texto_para_llm(text))


def strip_accents(text: str) -> str:
    if not text:
        return ""
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def search_first(patterns: List[str], text: str, flags: int = re.IGNORECASE | re.DOTALL):
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return match
    return None


def cut_text(text: str, limit: int = 18000) -> str:
    return text[:limit] if len(text) > limit else text


def is_forbidden_contractor_number(candidate: str) -> bool:
    c = only_digits(candidate)
    return c in FORBIDDEN_CONTRACTOR_NUMBERS


def looks_like_person_name(value: str) -> bool:
    if not value:
        return False
    value = re.sub(r"\s+", " ", value).strip(" ,.;:\n\t")
    words = [w for w in value.split() if w]
    if len(words) < 2:
        return False
    upper_words = sum(1 for w in words if re.fullmatch(r"[A-ZÁÉÍÓÚÑ]+(?:[-'][A-ZÁÉÍÓÚÑ]+)?", w))
    return upper_words >= min(2, len(words))


def looks_like_entity_name(value: str) -> bool:
    if not value:
        return False
    v = value.upper()
    entity_markers = [
        "S.A.S", "S.A.", "LTDA", "E.S.P", "UNIVERSIDAD", "CORPORACIÓN", "CORPORACION",
        "FUNDACIÓN", "FUNDACION", "CAJA DE COMPENSACIÓN", "CAJA DE COMPENSACION",
        "EMPRESA", "ASOCIACIÓN", "ASOCIACION", "COLEGIO", "INSTITUTO", "ETB", "CAFAM",
        "NACIONAL", "DISTRITAL", "UNIÓN TEMPORAL", "UNION TEMPORAL", "CONSORCIO",
        "FONDO", "ALIANZA", "FUNDACION UNIVERSITARIA", "PONTIFICIA", "POLITÉCNICO",
        "POLITECNICO", "ROSARIO", "UNISALLE", "UNAD", "FUCS", "BOSQUE", "S.A", "S EN C",
    ]
    return any(marker in v for marker in entity_markers)


def clean_contract_name(value: str) -> str:
    value = normalize_nullable_text(value)
    value = re.sub(r"^(la|el)\s+otra,?\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^la\s+tecnolog[íi]a\s+y\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^señor(?:a)?\s+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s{2,}", " ", value)
    return value.strip(" ,.;:\n\t")


def upper_clean(text: str) -> str:
    return normalize_spaces(text).upper()


def parse_colombian_money_to_float(text: str) -> Optional[float]:
    if not text:
        return None
    s = text.upper()
    s = s.replace("$", "").replace("COP", "").replace("M/CTE", "")
    s = s.replace(" ", "")
    # 1.234.567,89  -> 1234567.89
    if re.search(r"\d+\.\d{3}(?:\.\d{3})*,\d{2}$", s):
        s = s.replace(".", "").replace(",", ".")
    else:
        # 1,234,567.89  o  1234567.89
        if "," in s and "." in s and s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return None


def infer_tipo_documento_contratista(nombre_contratista: str, numero: str, contexto: str = "") -> str:
    if not numero:
        return ""
    if looks_like_entity_name(nombre_contratista):
        return "NIT"
    ctx = upper_clean(contexto)
    if "CÉDULA" in ctx or "CEDULA" in ctx or "C.C" in ctx:
        return "CC"
    if "NIT" in ctx:
        return "NIT"
    if len(numero) == 9:
        return "NIT"
    if len(numero) in (8, 10):
        return "CC"
    return ""


def standardize_date_text(value: str) -> str:
    return normalize_nullable_text(value).replace("  ", " ")


def month_name_to_number(name: str) -> Optional[int]:
    m = strip_accents(name.lower()).strip()
    mapping = {
        "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
        "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
        "noviembre": 11, "diciembre": 12
    }
    return mapping.get(m)


def extract_first_email(text: str) -> str:
    m = re.search(r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[A-Za-z]{2,}\b", text)
    return m.group(0) if m else ""


def extract_first_phone(text: str) -> str:
    patterns = [
        r"(?:tel[eé]fono|celular|m[óo]vil|tel\.?)\s*[:#]?\s*(\+?\d[\d\-\s]{6,20}\d)",
        r"\b(\+?57[\s\-]?\d{10})\b",
        r"\b(3\d{9})\b",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    value = re.sub(r"[^\d+]", "", m.group(1))
    return value


# =========================================================
# PDF
# =========================================================
def extract_text_with_pymupdf(pdf_bytes: bytes) -> str:
    if fitz is None:
        return ""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            try:
                pages.append(page.get_text("text") or "")
            except Exception:
                pages.append("")
        return "\n".join(pages)
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
    text_alt = extract_text_with_pypdf(pdf_bytes)
    if len(normalize_text(text_alt)) > len(normalize_text(text)):
        text = text_alt
    return text


# =========================================================
# CLASIFICACIÓN
# =========================================================
def classify_document(text: str, filename: str = "") -> str:
    txt = (filename + "\n" + text[:5000]).upper()
    if "MEMORANDO" in txt:
        return "memorando"
    if "DOCUMENTOS PREVIOS" in txt or "SOLICITUD ORDENACIÓN DE CONTRATACIÓN" in txt or "SOLICITUD ORDENACION DE CONTRATACION" in txt:
        return "estudios_previos"
    if "ACTO ADMINISTRATIVO" in txt or "MEDIANTE EL CUAL SE JUSTIFICA" in txt or "POR LA CUAL SE JUSTIFICA" in txt or "JUSTIFICACIÓN" in txt or "JUSTIFICACION" in txt:
        return "acto_justificacion"
    if "INFORME DE VERIFICACIÓN" in txt or "INFORME DE VERIFICACION" in txt or "PROPONENTE" in txt or "EVALUACION" in txt:
        return "evaluacion"
    if "CONTRATO" in txt or "CONVENIO" in txt or "CLAUSULADO" in txt or "MINUTA" in txt:
        return "contractual"
    return "otro"


# =========================================================
# EXTRACCIÓN BASE
# =========================================================
def extract_numero_proceso(text: str, filename: str = "") -> str:
    patterns = [
        r"(?:proceso(?:\s+de\s+selecci[oó]n)?|n[uú]mero\s+del\s+proceso|proceso\s*secop)\s*(?:No\.?|N[o°º]?\.?|#|:)?\s*([A-Z0-9\-_]{6,})",
        r"\b(CO1\.[A-Z]+\.\d{5,})\b",
        r"\b(CO1\.[A-Z]+\.\d+)\b",
    ]
    m = search_first(patterns, text)
    if m:
        return normalize_nullable_text(m.group(1))
    m_file = re.search(r"(CO1\.[A-Z]+\.\d+)", filename, re.IGNORECASE)
    if m_file:
        return m_file.group(1)
    return ""


def extract_contract_number(text: str, filename: str = "") -> str:
    patterns = [
        r"(?:CONTRATO|CONVENIO)[^\n]{0,120}?No\.?\s*([A-Z0-9\-_/]+(?:\s*[-–]\s*\d{4})?)",
        r"No\.?\s*(ATENEA\s*[-–]\s*\d+\s*[-–]\s*\d{4})",
        r"(ATENEA\s*[-–]\s*\d+\s*[-–]\s*\d{4})",
        r"(CO1PCCNTR\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            value = re.sub(r"\s+", "", m.group(1)).replace("–", "-")
            return value.strip(" .,:;\n\t")
    m_file = re.search(r"(ATENEA[-_]\d+[-_]\d{4})", filename, re.IGNORECASE)
    if m_file:
        return m_file.group(1).replace("_", "-")
    return ""


def extract_contract_type(text: str) -> str:
    patterns = [
        r"(?:CONTRATO|CLAUSULADO DEL CONTRATO)\s+DE\s+([A-ZÁÉÍÓÚÑ\s]+?)\s+No\.?",
        r"CONVENIO\s+([A-ZÁÉÍÓÚÑ\s]+?)\s+No\.?",
        r"presente\s+contrato\s+de\s+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+el\s+cual|\s+que\s+se\s+regir[áa]|\s+de\s+conformidad)",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    return re.sub(r"\s+", " ", m.group(1)).strip(" ,.;:\n\t").upper()


def extract_modalidad(text: str) -> str:
    modalidades = [
        "CONTRATACIÓN DIRECTA",
        "CONTRATACION DIRECTA",
        "LICITACIÓN PÚBLICA",
        "LICITACION PUBLICA",
        "SELECCIÓN ABREVIADA",
        "SELECCION ABREVIADA",
        "MÍNIMA CUANTÍA",
        "MINIMA CUANTIA",
        "CONCURSO DE MÉRITOS",
        "CONCURSO DE MERITOS",
        "SUBASTA INVERSA",
    ]
    upper = upper_clean(text[:15000])
    for m in modalidades:
        if m in upper:
            return m
    patterns = [
        r"modalidad\s+de\s+selecci[oó]n\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ\s]+)",
    ]
    m = search_first(patterns, text[:15000])
    if m:
        return normalize_nullable_text(m.group(1)).upper()
    return ""


def get_party_block(text: str) -> str:
    patterns = [
        r"(?:por\s+la\s+otra(?:\s+parte)?[,:\s]+)(.{0,2500}?)(?:EL\s+CONTRATISTA|LA\s+ENTIDAD\s+EJECUTORA|LA\s+ASOCIADA|LA\s+CONTRATISTA|previas\s+las\s+siguientes|CONSIDERACIONES|PRIMERA\s*:)",
        r"(?:y\s+por\s+la\s+otra(?:\s+parte)?[,:\s]+)(.{0,2500}?)(?:EL\s+CONTRATISTA|LA\s+ENTIDAD\s+EJECUTORA|LA\s+ASOCIADA|LA\s+CONTRATISTA|previas\s+las\s+siguientes|CONSIDERACIONES|PRIMERA\s*:)",
        r"(?:actuando\s+en\s+nombre\s+y\s+representación\s+de\s+)(.{0,500}?)(?:,?\s+quien\s+en\s+adelante|\s+con\s+NIT)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            return normalize_spaces(m.group(1))
    return ""


def extract_name_from_header(text: str) -> str:
    patterns = [
        r"celebrado\s+entre\s+[^\n]{0,220}?\s+y\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?)\.",
        r"entre\s+la\s+AGENCIA[^\n]{0,260}?\s+y\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?)\.",
        r"entre\s+la\s+AGENCIA[^\n]{0,260}?\s+y\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?)(?:,|\.)",
        r"actuando\s+en\s+nombre\s+y\s+representación\s+de\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?),\s+con\s+NIT",
    ]
    m = search_first(patterns, text[:4500])
    if not m:
        return ""
    return clean_contract_name(m.group(1))


def extract_name_from_party_block(text: str) -> str:
    block = get_party_block(text) or text[:10000]
    patterns = [
        r"la\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?),\s*con\s+NIT",
        r"el\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?),\s*con\s+NIT",
        r"([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?),\s*mayor\s+de\s+edad",
        r"([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?),\s*identificad[oa]",
        r"actuando\s+en\s+nombre\s+y\s+representación\s+de\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?)(?:,|\s+con\s+NIT)",
        r"representación\s+de\s+([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?)(?:,|\s+con\s+NIT)",
    ]
    for pat in patterns:
        m = re.search(pat, block, re.IGNORECASE | re.DOTALL)
        if m:
            val = clean_contract_name(m.group(1))
            if val and "AGENCIA ATENEA" not in val.upper():
                return val
    return ""


def extract_name_from_role_patterns(text: str) -> str:
    patterns = [
        r"quien\s+en\s+adelante\s+se\s+denominar[áa]\s+EL\s+CONTRATISTA.*?por\s+la\s+otra,\s*([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]+?),",
        r"LA\s+ENTIDAD\s+EJECUTORA.*?([A-ZÁÉÍÓÚÑ0-9\.\-\s]+?),\s*con\s+NIT",
    ]
    m = search_first(patterns, text[:12000])
    if not m:
        return ""
    return clean_contract_name(m.group(1))


def extract_contractor_name(text: str, filename: str = "") -> str:
    candidates = []
    for candidate in [
        extract_name_from_header(text),
        extract_name_from_party_block(text),
        extract_name_from_role_patterns(text),
    ]:
        if candidate:
            candidates.append(candidate)

    if candidates:
        candidates = [c for c in dict.fromkeys(candidates) if c]
        entity_candidates = [c for c in candidates if looks_like_entity_name(c)]
        person_candidates = [c for c in candidates if looks_like_person_name(c)]
        if entity_candidates:
            return entity_candidates[0]
        if person_candidates:
            return person_candidates[0]
        return candidates[0]

    stem = Path(filename).stem.upper()
    m = re.search(r"MINUTA\s+(?:CONTRACTUAL|CLAUSULADO)?\s*(.+)$", stem)
    if m:
        val = clean_contract_name(m.group(1).replace("ATENEA", "").replace("-", " "))
        if val:
            return val
    return ""


def build_document_candidates(text: str, contractor_name: str = "") -> List[dict]:
    text_norm = normalize_spaces(text)
    candidates: List[dict] = []

    num_pattern = r"([0-9OIl][0-9OIl\.\,\-\s]{5,}[0-9OIl])"
    contractor_name_norm = normalize_spaces(contractor_name)

    def add_candidate(raw_value: str, source: str, context: str, preferred_type: str = ""):
        value = only_digits(raw_value)
        if not value:
            return
        if len(value) < 6 or len(value) > 12:
            return
        candidates.append(
            {
                "value": value,
                "source": source,
                "context": normalize_spaces(context)[:500],
                "preferred_type": preferred_type,
            }
        )

    party_block = get_party_block(text_norm)
    if party_block:
        local_patterns = [
            (r"NIT\s*(?:No\.?|#|:)?\s*([0-9\.\-\s]{6,25})", "party_block_nit", "nit"),
            (r"c[ée]dula\s+de\s+ciudadan[íi]a\s*(?:No\.?|N°|Nº|#|:)?\s*([0-9\.\-\s]{6,25})", "party_block_cc", "cc"),
            (r"\bC\.?\s*C\.?\s*(?:No\.?|N°|Nº|#|:)?\s*([0-9\.\-\s]{6,25})", "party_block_cc_abbr", "cc"),
            (r"identificad[oa][^\n]{0,120}?([0-9\.\-\s]{6,25})", "party_block_identificada", ""),
        ]
        for pat, source, preferred_type in local_patterns:
            for m in re.finditer(pat, party_block, re.IGNORECASE):
                add_candidate(m.group(1), source, party_block, preferred_type)

    if contractor_name_norm:
        m_name = re.search(re.escape(contractor_name_norm), text_norm, re.IGNORECASE)
        if m_name:
            start = max(0, m_name.start() - 180)
            end = min(len(text_norm), m_name.end() + 520)
            window = text_norm[start:end]
            local_patterns = [
                (rf"{re.escape(contractor_name_norm)}.{{0,180}}?NIT\s*(?:No\.?|#|:)?\s*{num_pattern}", "name_window_nit", "nit"),
                (rf"{re.escape(contractor_name_norm)}.{{0,180}}?c[ée]dula\s+de\s+ciudadan[íi]a\s*(?:No\.?|N°|Nº|#|:)?\s*{num_pattern}", "name_window_cc", "cc"),
                (rf"{re.escape(contractor_name_norm)}.{{0,180}}?\bC\.?\s*C\.?\s*(?:No\.?|N°|Nº|#|:)?\s*{num_pattern}", "name_window_cc_abbr", "cc"),
                (rf"{re.escape(contractor_name_norm)}.{{0,220}}?identificad[oa].{{0,80}}?{num_pattern}", "name_window_identificada", ""),
            ]
            for pat, source, preferred_type in local_patterns:
                for m in re.finditer(pat, window, re.IGNORECASE | re.DOTALL):
                    add_candidate(m.group(1), source, window, preferred_type)

    rep_patterns = [
        (r"representada\s+legalmente\s+por\s+[^\n]{0,200}?c[ée]dula\s+de\s+ciudadan[íi]a\s*(?:No\.?|#|:)?\s*([0-9\.\-\s]{6,25})", "representante_cc", "cc"),
        (r"actuando\s+en\s+nombre\s+y\s+representación\s+de\s+[^\n]{0,180}?con\s+NIT\s*(?:No\.?|#|:)?\s*([0-9\.\-\s]{6,25})", "entity_after_rep_nit", "nit"),
    ]
    head = text_norm[:15000]
    for pat, source, preferred_type in rep_patterns:
        for m in re.finditer(pat, head, re.IGNORECASE | re.DOTALL):
            context = head[max(0, m.start()-150):min(len(head), m.end()+150)]
            add_candidate(m.group(1), source, context, preferred_type)

    return candidates


def score_document_candidate(candidate: dict, contractor_name: str = "") -> int:
    value = candidate.get("value", "")
    context = (candidate.get("context", "") or "").upper()
    preferred_type = candidate.get("preferred_type", "")
    source = candidate.get("source", "")

    if not value:
        return -999
    if is_forbidden_contractor_number(value):
        return -1000

    score = 0

    if source.startswith("party_block"):
        score += 6
    if source.startswith("name_window"):
        score += 5
    if "REPRESENTACIÓN DE" in context or "REPRESENTACION DE" in context:
        score += 2

    if any(term in context for term in FORBIDDEN_DOC_CONTEXT_TERMS):
        if "POR LA OTRA" not in context and "REPRESENTADA LEGALMENTE" not in context and "ACTUANDO EN NOMBRE Y REPRESENTACIÓN DE" not in context and "ACTUANDO EN NOMBRE Y REPRESENTACION DE" not in context:
            score -= 8

    if 8 <= len(value) <= 10:
        score += 2
    if len(value) == 9:
        score += 2
    if len(value) == 10:
        score += 1

    if looks_like_entity_name(contractor_name):
        if preferred_type == "nit":
            score += 4
        if len(value) == 9:
            score += 4
        if len(value) == 10:
            score += 2
    elif looks_like_person_name(contractor_name):
        if preferred_type == "cc":
            score += 4
        if len(value) in (8, 10):
            score += 4
        if len(value) == 9 and preferred_type != "cc":
            score -= 2
    else:
        if preferred_type == "nit":
            score += 1
        if preferred_type == "cc":
            score += 1

    if value.startswith("901508361"):
        score -= 50

    return score


def extract_contractor_document(text: str, contractor_name: str = "") -> Tuple[str, str]:
    candidates = build_document_candidates(text, contractor_name)
    if not candidates:
        return "", ""
    ranked = sorted(
        candidates,
        key=lambda c: (score_document_candidate(c, contractor_name), len(c.get("value", ""))),
        reverse=True,
    )
    top = ranked[0]
    value = top["value"]
    if is_forbidden_contractor_number(value):
        return "", ""
    tipo = infer_tipo_documento_contratista(contractor_name, value, top.get("context", ""))
    return value, tipo


def extract_obligaciones_especificas(text: str) -> str:
    start_patterns = [
        r"B\)\s*OBLIGACIONES\s+ESPEC[ÍI]FICAS\s*:\s*A\s*EL\s+CONTRATISTA\s+le\s+corresponde\s+el\s+cumplimiento\s+de\s+las\s+siguientes\s+obligaciones\s*:",
        r"B\)\s*OBLIGACIONES\s+ESPEC[ÍI]FICAS\s*:",
        r"OBLIGACIONES\s+ESPEC[ÍI]FICAS\s*:",
    ]
    end_patterns = [
        r"C\)\s*OBLIGACIONES\s+DE\s+LA\s+CONTRATANTE",
        r"OBLIGACIONES\s+DE\s+LA\s+CONTRATANTE",
        r"CL[ÁA]USULA\s+TERCERA\s*:",
        r"CL[ÁA]USULA\s+CUARTA\s*:",
        r"CL[ÁA]USULA\s+DE\s+SUPERVISI[ÓO]N",
    ]
    start_match = search_first(start_patterns, text)
    if not start_match:
        return ""
    tail = text[start_match.end():]
    end_match = search_first(end_patterns, tail)
    obligaciones = tail[: end_match.start()] if end_match else tail
    obligaciones = obligaciones.strip()
    obligaciones = re.sub(r"\n{3,}", "\n\n", obligaciones)
    return obligaciones.strip()


def extract_supervisor_name(text: str) -> str:
    patterns = [
        r"(?:la\s+)?supervisi[óo]n\s+(?:del\s+presente\s+contrato|contractual)?\s*(?:ser[áa]\s+ejercida|estar[áa]\s+a\s+cargo|corresponder[áa])\s+por\s+([^\n\.]+)",
        r"supervisor(?:a)?\s+del\s+contrato\s*(?:ser[áa]|es|:)?\s*([^\n\.]+)",
        r"la\s+supervisi[óo]n\s+ser[áa]\s+ejercida\s+por\s+([^\n\.]+)",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    value = m.group(1)
    value = re.split(r";|,\s*quien\s+|,\s*o\s+por\s+quien|\s+o\s+por\s+quien", value, maxsplit=1, flags=re.IGNORECASE)[0]
    value = re.sub(r"\s+", " ", value).strip(" ,.;:\n\t")
    value = re.sub(r"^(el|la|los|las)\s+", "", value, flags=re.IGNORECASE)
    return value


def extract_objeto(text: str) -> str:
    patterns = [
        r"(?:OBJETO|OBJETO DEL CONTRATO|OBJETO A CONTRATAR)\s*[:\-]?\s*(.{20,2000}?)(?:\n[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{3,}:|\n(?:ALCANCE|PLAZO|VALOR|OBLIGACIONES|SUPERVISI[ÓO]N|FORMA DE PAGO|CL[ÁA]USULA))",
        r"el\s+objeto\s+del\s+presente\s+contrato\s+es\s*(.{20,1800}?)(?:\n[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{3,}:|\n(?:PLAZO|VALOR|OBLIGACIONES|SUPERVISI[ÓO]N|FORMA DE PAGO|CL[ÁA]USULA))",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    return normalize_nullable_text(m.group(1))


def extract_nombre_contratante(text: str) -> str:
    patterns = [
        r"(AGENCIA\s+DISTRITAL\s+PARA\s+LA\s+EDUCACI[ÓO]N\s+SUPERIOR,\s+LA\s+CIENCIA\s+Y\s+LA\s+TECNOLOG[ÍI]A\s+[–-]?\s*ATENEA)",
        r"(AGENCIA\s+ATENEA)",
    ]
    m = search_first(patterns, text[:7000])
    if m:
        return normalize_nullable_text(m.group(1))
    return ""


def extract_nit_contratante(text: str) -> str:
    patterns = [
        r"(?:NIT\s*(?:No\.?|#|:)?\s*)(901[\d\.\- ]{5,})",
    ]
    matches = re.finditer(patterns[0], text[:10000], re.IGNORECASE)
    for m in matches:
        value = only_digits(m.group(1))
        if value.startswith("901508361"):
            return value
    return ""


def extract_valor_contrato_text(text: str) -> str:
    patterns = [
        r"(?:VALOR(?:\s+TOTAL)?\s+DEL\s+CONTRATO|VALOR\s+DEL\s+CONTRATO|CUANT[ÍI]A)\s*[:\-]?\s*(\$[\d\.\,\s]+(?:M/CTE)?(?:\s*COP)?)",
        r"(?:por\s+valor\s+de)\s*(\$[\d\.\,\s]+(?:M/CTE)?(?:\s*COP)?)",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    return normalize_nullable_text(m.group(1))


def extract_plazo_text(text: str) -> str:
    patterns = [
        r"(?:PLAZO\s+DE\s+EJECUCI[ÓO]N|PLAZO)\s*[:\-]?\s*(.{5,400}?)(?:\n[A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s]{3,}:|\n(?:VALOR|FORMA DE PAGO|SUPERVISI[ÓO]N|OBLIGACIONES|CL[ÁA]USULA))",
        r"el\s+plazo\s+de\s+ejecuci[oó]n\s+ser[áa]\s+de\s+(.{5,250}?)(?:\.|\n)",
    ]
    m = search_first(patterns, text)
    if not m:
        return ""
    return normalize_nullable_text(m.group(1))


def infer_plazo_dias(plazo_texto: str) -> Optional[int]:
    if not plazo_texto:
        return None
    t = strip_accents(plazo_texto.lower())

    total = 0
    found = False

    patterns = [
        (r"(\d+)\s+dias?", 1),
        (r"(\d+)\s+mes(?:es)?", 30),
        (r"(\d+)\s+anos?", 365),
        (r"(\d+)\s+semanas?", 7),
    ]
    for pat, factor in patterns:
        for m in re.finditer(pat, t):
            total += int(m.group(1)) * factor
            found = True

    if found:
        return total

    word_map = {
        "un": 1, "uno": 1, "dos": 2, "tres": 3, "cuatro": 4, "cinco": 5, "seis": 6,
        "siete": 7, "ocho": 8, "nueve": 9, "diez": 10, "once": 11, "doce": 12,
    }
    for word, num in word_map.items():
        if re.search(rf"\b{word}\s+mes(?:es)?\b", t):
            return num * 30
        if re.search(rf"\b{word}\s+dias?\b", t):
            return num
    return None


def extract_generic_date_by_label(text: str, labels: List[str]) -> str:
    label_group = "|".join(labels)
    patterns = [
        rf"(?:{label_group})\s*[:\-]?\s*(\d{{1,2}}[\/\-]\d{{1,2}}[\/\-]\d{{2,4}})",
        rf"(?:{label_group})\s*[:\-]?\s*(\d{{4}}[\/\-]\d{{1,2}}[\/\-]\d{{1,2}})",
        rf"(?:{label_group})\s*[:\-]?\s*(\d{{1,2}}\s+de\s+[A-Za-záéíóúñÁÉÍÓÚÑ]+\s+de\s+\d{{4}})",
    ]
    m = search_first(patterns, text)
    return standardize_date_text(m.group(1)) if m else ""


def extract_fecha_suscripcion(text: str) -> str:
    return extract_generic_date_by_label(
        text,
        ["fecha\\s+de\\s+(?:suscripci[oó]n|firma)", "suscrito\\s+el", "firmado\\s+el"],
    )


def extract_fecha_inicio(text: str) -> str:
    return extract_generic_date_by_label(
        text,
        ["fecha\\s+de\\s+inicio", "inici[oó]\\s+del\\s+contrato"],
    )


def extract_fecha_terminacion(text: str) -> str:
    return extract_generic_date_by_label(
        text,
        ["fecha\\s+de\\s+terminaci[oó]n", "fecha\\s+final", "vence\\s+el"],
    )


def extract_cdp(text: str) -> str:
    patterns = [
        r"(?:CDP|CERTIFICADO\s+DE\s+DISPONIBILIDAD\s+PRESUPUESTAL)\s*(?:No\.?|N[o°º]?\.?|#|:)?\s*([A-Z0-9\-\/]+)",
    ]
    m = search_first(patterns, text)
    return normalize_nullable_text(m.group(1)) if m else ""


def extract_rp(text: str) -> str:
    patterns = [
        r"(?:RP|REGISTRO\s+PRESUPUESTAL)\s*(?:No\.?|N[o°º]?\.?|#|:)?\s*([A-Z0-9\-\/]+)",
    ]
    m = search_first(patterns, text)
    return normalize_nullable_text(m.group(1)) if m else ""


def extract_unspsc(text: str) -> str:
    upper = upper_clean(text)
    values = set()

    for m in re.finditer(r"\b(\d{8})\b", upper):
        value = m.group(1)
        if value.startswith(("10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                             "20", "21", "22", "23", "24", "25", "26", "27", "30", "31",
                             "32", "39", "40", "41", "42", "43", "44", "45", "46", "47",
                             "48", "49", "50", "51", "52", "53", "54", "55", "56", "57",
                             "60", "70", "71", "72", "73", "76", "78", "80", "81", "82",
                             "83", "84", "85", "86", "90", "92", "93", "94", "95")):
            values.add(value)

    if values:
        return "; ".join(sorted(values))
    return ""


# =========================================================
# IA
# =========================================================
def get_openai_client(api_key: Optional[str]) -> Optional[OpenAI]:
    if not api_key:
        return None
    try:
        return OpenAI(api_key=api_key)
    except Exception:
        return None


def build_focus_context(text: str, rule_result: dict) -> str:
    parts = []
    parts.append("=== INICIO DEL DOCUMENTO ===\n" + cut_text(text[:10000], 10000))

    party_block = get_party_block(text)
    if party_block:
        parts.append("=== BLOQUE PRINCIPAL DEL CONTRATISTA ===\n" + cut_text(party_block, 3500))

    if rule_result.get("objeto"):
        parts.append("=== OBJETO DETECTADO POR REGLAS ===\n" + cut_text(rule_result["objeto"], 2000))

    if rule_result.get("obligaciones_especificas"):
        parts.append("=== OBLIGACIONES DETECTADAS POR REGLAS ===\n" + cut_text(rule_result["obligaciones_especificas"], 7000))

    parts.append("=== CANDIDATOS POR REGLAS ===\n" + json.dumps(rule_result, ensure_ascii=False, indent=2))
    return "\n\n".join(parts)


def call_ai_extraction(client: OpenAI, text: str, filename: str, rule_result: dict, doc_class: str) -> dict:
    prompt = f"""
Analiza el siguiente documento contractual en español y devuelve SOLO JSON válido con estos campos exactos:
- numero_proceso
- numero_contrato
- tipo_contrato
- modalidad
- objeto
- nombre_contratante
- nit_contratante
- nombre_contratista
- numero_documento_contratista
- tipo_documento_contratista
- correo_contratista
- telefono_contratista
- supervisor
- fecha_suscripcion
- fecha_inicio
- fecha_terminacion
- plazo_texto
- valor_contrato_texto
- cdp
- rp
- codigo_unspsc
- obligaciones_especificas

Reglas:
1. No inventes datos.
2. Si no aparece claramente, devuelve "".
3. nombre_contratista debe ser la persona o entidad contratista, NO la contratante.
4. numero_documento_contratista debe quedar SOLO con dígitos.
5. El valor 901508361 o 9015083614 NO debe devolverse como documento del contratista.
6. Si el contratista es persona jurídica, prioriza su NIT; si es persona natural, prioriza su cédula.
7. codigo_unspsc puede venir como varios códigos separados por "; ".
8. obligaciones_especificas debe conservar el texto, no resumirse.
9. Devuelve solo JSON válido, sin markdown.

Archivo: {filename}
Tipo documental detectado: {doc_class}

Contexto:
{build_focus_context(text, rule_result)}
"""
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Devuelve SOLO JSON válido."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    return safe_json_loads(resp.choices[0].message.content)


def normalize_ai_result(data: dict) -> dict:
    data = data or {}
    out = {}
    for field in TARGET_FIELDS:
        if field in {"archivo", "tipo_documento_origen", "fuente_extraccion", "error", "plazo_dias_inferido", "valor_contrato_num"}:
            continue
        out[field] = normalize_nullable_text(data.get(field, ""))
    out["numero_documento_contratista"] = only_digits(out.get("numero_documento_contratista", ""))
    out["nit_contratante"] = only_digits(out.get("nit_contratante", ""))
    if is_forbidden_contractor_number(out.get("numero_documento_contratista", "")):
        out["numero_documento_contratista"] = ""
    return out


def merge_results(rule_result: dict, ai_result: Optional[dict]) -> dict:
    result = dict(rule_result)

    if not ai_result:
        result["fuente_extraccion"] = "reglas"
        return result

    ai = normalize_ai_result(ai_result)

    overwrite_if_empty = [
        "numero_proceso", "numero_contrato", "tipo_contrato", "modalidad", "objeto",
        "nombre_contratante", "nit_contratante", "correo_contratista", "telefono_contratista",
        "supervisor", "fecha_suscripcion", "fecha_inicio", "fecha_terminacion",
        "plazo_texto", "valor_contrato_texto", "cdp", "rp", "codigo_unspsc",
    ]
    for field in overwrite_if_empty:
        if not result.get(field) and ai.get(field):
            result[field] = ai[field]

    if ai.get("nombre_contratista"):
        if (
            not result.get("nombre_contratista")
            or (looks_like_entity_name(ai["nombre_contratista"]) and not looks_like_entity_name(result.get("nombre_contratista", "")))
            or len(ai["nombre_contratista"]) > len(result.get("nombre_contratista", "")) + 6
        ):
            result["nombre_contratista"] = ai["nombre_contratista"]

    if ai.get("numero_documento_contratista"):
        if (
            not result.get("numero_documento_contratista")
            or is_forbidden_contractor_number(result.get("numero_documento_contratista", ""))
        ):
            result["numero_documento_contratista"] = ai["numero_documento_contratista"]

    if ai.get("tipo_documento_contratista") and not result.get("tipo_documento_contratista"):
        result["tipo_documento_contratista"] = ai["tipo_documento_contratista"]

    if ai.get("obligaciones_especificas"):
        if len(result.get("obligaciones_especificas", "")) < 80 or len(ai["obligaciones_especificas"]) > len(result.get("obligaciones_especificas", "")):
            result["obligaciones_especificas"] = ai["obligaciones_especificas"]

    if ai.get("valor_contrato_texto") and not result.get("valor_contrato_texto"):
        result["valor_contrato_texto"] = ai["valor_contrato_texto"]

    result["fuente_extraccion"] = "hibrido_reglas_ia"
    return result


# =========================================================
# PROCESAMIENTO
# =========================================================
def extract_rule_result(raw_text: str, filename: str) -> dict:
    text = normalize_text(raw_text)
    doc_class = classify_document(text, filename)

    contractor_name = extract_contractor_name(text, filename)
    contractor_doc, contractor_doc_type = extract_contractor_document(text, contractor_name)

    rule_result = {
        "archivo": Path(filename).name,
        "tipo_documento_origen": doc_class,
        "numero_proceso": extract_numero_proceso(text, filename),
        "numero_contrato": extract_contract_number(text, filename),
        "tipo_contrato": extract_contract_type(text),
        "modalidad": extract_modalidad(text),
        "objeto": extract_objeto(raw_text or text),
        "nombre_contratante": extract_nombre_contratante(text),
        "nit_contratante": extract_nit_contratante(text),
        "nombre_contratista": contractor_name,
        "numero_documento_contratista": contractor_doc,
        "tipo_documento_contratista": contractor_doc_type,
        "correo_contratista": extract_first_email(text),
        "telefono_contratista": extract_first_phone(text),
        "supervisor": extract_supervisor_name(text),
        "fecha_suscripcion": extract_fecha_suscripcion(text),
        "fecha_inicio": extract_fecha_inicio(text),
        "fecha_terminacion": extract_fecha_terminacion(text),
        "plazo_texto": extract_plazo_text(text),
        "plazo_dias_inferido": None,
        "valor_contrato_texto": extract_valor_contrato_text(text),
        "valor_contrato_num": None,
        "cdp": extract_cdp(text),
        "rp": extract_rp(text),
        "codigo_unspsc": extract_unspsc(text),
        "obligaciones_especificas": extract_obligaciones_especificas(raw_text or text),
        "fuente_extraccion": "reglas",
        "error": "",
    }

    rule_result["plazo_dias_inferido"] = infer_plazo_dias(rule_result.get("plazo_texto", ""))
    rule_result["valor_contrato_num"] = parse_colombian_money_to_float(rule_result.get("valor_contrato_texto", ""))

    if not rule_result["tipo_documento_contratista"]:
        rule_result["tipo_documento_contratista"] = infer_tipo_documento_contratista(
            rule_result.get("nombre_contratista", ""),
            rule_result.get("numero_documento_contratista", ""),
            text[:4000],
        )

    return rule_result


def process_single_pdf(pdf_bytes: bytes, filename: str, client: Optional[OpenAI] = None, use_ai: bool = True) -> Dict:
    raw_text = extract_text_from_pdf_bytes(pdf_bytes)
    if not normalize_text(raw_text):
        return {
            "archivo": Path(filename).name,
            "tipo_documento_origen": "error",
            "numero_proceso": "",
            "numero_contrato": "",
            "tipo_contrato": "",
            "modalidad": "",
            "objeto": "",
            "nombre_contratante": "",
            "nit_contratante": "",
            "nombre_contratista": "",
            "numero_documento_contratista": "",
            "tipo_documento_contratista": "",
            "correo_contratista": "",
            "telefono_contratista": "",
            "supervisor": "",
            "fecha_suscripcion": "",
            "fecha_inicio": "",
            "fecha_terminacion": "",
            "plazo_texto": "",
            "plazo_dias_inferido": None,
            "valor_contrato_texto": "",
            "valor_contrato_num": None,
            "cdp": "",
            "rp": "",
            "codigo_unspsc": "",
            "obligaciones_especificas": "",
            "fuente_extraccion": "sin_texto",
            "error": "No fue posible extraer texto del PDF",
        }

    rule_result = extract_rule_result(raw_text, filename)
    final_result = dict(rule_result)

    if use_ai and client is not None:
        try:
            ai_result = call_ai_extraction(
                client=client,
                text=normalize_text(raw_text),
                filename=filename,
                rule_result=rule_result,
                doc_class=rule_result["tipo_documento_origen"],
            )
            final_result = merge_results(rule_result, ai_result)
            if not final_result.get("valor_contrato_num"):
                final_result["valor_contrato_num"] = parse_colombian_money_to_float(final_result.get("valor_contrato_texto", ""))
            if not final_result.get("plazo_dias_inferido"):
                final_result["plazo_dias_inferido"] = infer_plazo_dias(final_result.get("plazo_texto", ""))
        except Exception as e:
            final_result["fuente_extraccion"] = "reglas_con_fallo_ia"
            final_result["error"] = f"Fallo IA: {e}"

    return final_result


def process_zip(zip_path: Path, client: Optional[OpenAI] = None, use_ai: bool = True) -> List[Dict]:
    results = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        pdf_files = [name for name in zf.namelist() if name.lower().endswith(".pdf")]
        total = len(pdf_files)
        if total == 0:
            raise ValueError("El ZIP no contiene archivos PDF.")

        for i, name in enumerate(pdf_files, start=1):
            print(f"Procesando {i}/{total}: {Path(name).name}")
            pdf_bytes = zf.read(name)
            try:
                result = process_single_pdf(pdf_bytes, name, client=client, use_ai=use_ai)
                results.append(result)
            except Exception as e:
                results.append(
                    {
                        "archivo": Path(name).name,
                        "tipo_documento_origen": "error",
                        "numero_proceso": "",
                        "numero_contrato": "",
                        "tipo_contrato": "",
                        "modalidad": "",
                        "objeto": "",
                        "nombre_contratante": "",
                        "nit_contratante": "",
                        "nombre_contratista": "",
                        "numero_documento_contratista": "",
                        "tipo_documento_contratista": "",
                        "correo_contratista": "",
                        "telefono_contratista": "",
                        "supervisor": "",
                        "fecha_suscripcion": "",
                        "fecha_inicio": "",
                        "fecha_terminacion": "",
                        "plazo_texto": "",
                        "plazo_dias_inferido": None,
                        "valor_contrato_texto": "",
                        "valor_contrato_num": None,
                        "cdp": "",
                        "rp": "",
                        "codigo_unspsc": "",
                        "obligaciones_especificas": "",
                        "fuente_extraccion": "error",
                        "error": str(e),
                    }
                )
    return results


def save_results_to_excel(data: List[Dict], output_path: Path) -> None:
    df = pd.DataFrame(data)
    existing_cols = [c for c in TARGET_FIELDS if c in df.columns]
    others = [c for c in df.columns if c not in existing_cols]
    df = df[existing_cols + others]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="contratos")


def print_configuration(zip_path: Path, output_path: Path, use_ai: bool, api_key: str) -> None:
    print("=" * 88)
    print("EXTRACCIÓN ROBUSTA DE CONTRATOS SECOP A EXCEL")
    print("=" * 88)
    print(f"ZIP de entrada : {zip_path}")
    print(f"Excel de salida: {output_path}")
    print(f"Usar IA        : {'Sí' if use_ai else 'No'}")
    print(f"OpenAI API Key : {'Detectada' if api_key else 'No detectada'}")
    print("=" * 88)


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
        zip_path = Path(selected)
        if not zip_path.exists():
            print(f"No existe la ruta: {zip_path}")
            continue
        if not zip_path.is_file():
            print(f"La ruta no corresponde a un archivo: {zip_path}")
            continue
        if zip_path.suffix.lower() != ".zip":
            print(f"El archivo no es .zip: {zip_path.name}")
            continue
        return zip_path


def prompt_output_path(default_path: str) -> Path:
    while True:
        print("\nIngresa la ruta completa del Excel de salida.")
        print(f"Presiona Enter para usar la ruta por defecto: {default_path}")
        user_input = input("Ruta del Excel de salida (.xlsx): ").strip().strip('"')
        selected = user_input or default_path
        if not selected:
            print("Debes escribir una ruta de salida.")
            continue
        output_path = Path(selected)
        if output_path.suffix.lower() != ".xlsx":
            print("La ruta de salida debe terminar en .xlsx")
            continue
        return output_path


def ask_yes_no(message: str, default: bool = True) -> bool:
    hint = "S/n" if default else "s/N"
    answer = input(f"{message} [{hint}]: ").strip().lower()
    if not answer:
        return default
    return answer in {"s", "si", "sí", "y", "yes"}


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        zip_path = Path(sys.argv[1].strip().strip('"'))
    else:
        zip_path = prompt_zip_path(INPUT_ZIP_PATH)

    if len(sys.argv) >= 3 and sys.argv[2].strip():
        output_path = Path(sys.argv[2].strip().strip('"'))
    else:
        default_output = str(zip_path.with_name("secop_base_datos_robusta.xlsx")) if zip_path else OUTPUT_EXCEL_PATH
        output_path = prompt_output_path(default_output)

    use_ai = ask_yes_no("¿Quieres usar IA para fortalecer la extracción?", default=USE_AI)

    api_key = OPENAI_API_KEY.strip()
    client = get_openai_client(api_key) if use_ai and api_key else None
    if use_ai and not client:
        print("\nAdvertencia: no se detectó OPENAI_API_KEY válida. Se procesará solo con reglas.")
        use_ai = False

    print_configuration(zip_path, output_path, use_ai, api_key)
    data = process_zip(zip_path=zip_path, client=client, use_ai=use_ai)
    save_results_to_excel(data, output_path)
    print("\nProceso terminado correctamente.")
    print(f"Excel guardado en: {output_path}")


if __name__ == "__main__":
    main()
