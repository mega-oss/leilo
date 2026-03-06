#!/usr/bin/env python3
"""
leilo.py — Scraper leilo.com.br + upload direto para auctions.veiculos

Uso:
    # Scrapa UMA categoria e manda pro Supabase:
    python leilo.py "https://leilo.com.br/leilao/brasil/carros"

    # Scrapa TODAS as categorias de veículos:
    python leilo.py --all

    # Modo debug (sem upload):
    python leilo.py "URL" --no-upload --debug

    # Só links, sem entrar em cada lote:
    python leilo.py "URL" --no-details

    # Mostra browser:
    python leilo.py "URL" --show
"""

import asyncio
import json
import re
import sys
import argparse
import os
import io
import time
import requests as _requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from playwright.async_api import async_playwright, Response

# Adiciona scrapers/ ao path para importar supabase_client
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from supabase_client import SupabaseClient

# ─── Cores ────────────────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# ─── Mapeamento categoria URL → tipo DB ───────────────────────────────────────
CATEGORIAS = {
    "carros":      ("https://leilo.com.br/leilao/carros/de.2018/ate.2026"
                    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908",    "carro"),
    "motos":       ("https://leilo.com.br/leilao/motos/de.2018/ate.2026"
                    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908",    "moto"),
    "pesados":     ("https://leilo.com.br/leilao/pesados/de.2018/ate.2026"
                    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908",    "caminhao"),
    "utilitarios": ("https://leilo.com.br/leilao/utilitarios/de.2018/ate.2026"
                    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908",    "van"),
    "sucatas":     ("https://leilo.com.br/leilao/sucatas/de.2018/ate.2026"
                    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908",    "sucata"),
}


def _tipo_from_url(url: str) -> str:
    """Infere o tipo do veículo a partir da URL.
    Tipos válidos: carro, moto, caminhao, van, maquinario, sucata, outro
    """
    u = url.lower()
    for slug, (_, tipo) in CATEGORIAS.items():
        if slug in u:
            return tipo
    return "outro"


# ─── Helpers monetários ───────────────────────────────────────────────────────

def parse_brl(v) -> float | None:
    if v is None:
        return None
    s = str(v).replace("R$", "").replace("\xa0", "").replace(" ", "").strip()
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^\d+(,\d{1,2})$", s):
        s = s.replace(",", ".")
    s = s.replace(",", "")
    try:
        val = float(s)
        if 500 <= val <= 5_000_000:
            return val
    except Exception:
        pass
    return None


def pct_desconto(lance, mercado) -> float | None:
    l = parse_brl(lance)
    m = parse_brl(mercado)
    if l and m and m > 0:
        return round((1 - l / m) * 100, 1)
    return None


def fmt_brl(v) -> str:
    val = parse_brl(v)
    if val is None:
        return str(v) if v else "—"
    s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


# ─── Helpers de parsing ───────────────────────────────────────────────────────

def parse_ano(ano_str: str) -> tuple[int | None, int | None]:
    """
    '2019/2020' → (2019, 2020)
    '2019 2020' → (2019, 2020)
    '2019'      → (2019, 2019)
    """
    if not ano_str or ano_str == "—":
        return None, None
    nums = re.findall(r"\d{4}", str(ano_str))
    if len(nums) >= 2:
        fab, mod = int(nums[0]), int(nums[1])
        # sanidade
        if 1950 <= fab <= 2030 and fab <= mod <= fab + 2:
            return fab, mod
    if len(nums) == 1:
        ano = int(nums[0])
        if 1950 <= ano <= 2030:
            return ano, ano
    return None, None


def parse_km(km_str) -> int | None:
    if not km_str or km_str == "—":
        return None
    s = re.sub(r"[^\d]", "", str(km_str))
    try:
        val = int(s)
        return val if 0 <= val <= 2_000_000 else None
    except Exception:
        return None


def parse_localizacao(loc: str) -> tuple[str | None, str | None]:
    """
    'Goiânia - GO' → (estado='GO', cidade='Goiânia')
    'São Paulo/SP'  → (estado='SP', cidade='São Paulo')
    """
    if not loc or loc == "—":
        return None, None
    # Tenta padrão "Cidade - UF" ou "Cidade/UF"
    m = re.search(r"(.+?)\s*[-/]\s*([A-Z]{2})\s*$", loc.strip())
    if m:
        return m.group(2).upper(), m.group(1).strip()
    # Só UF
    m2 = re.search(r"\b([A-Z]{2})\b", loc)
    if m2:
        return m2.group(1), None
    return None, None


def parse_marca_modelo(titulo: str) -> tuple[str | None, str | None]:
    """
    Tenta extrair marca e modelo do título do veículo.
    Heurística simples: primeira palavra maiúscula → marca.
    """
    if not titulo or titulo == "—":
        return None, None
    partes = titulo.strip().split()
    if not partes:
        return None, None
    marca = partes[0].upper()
    modelo = " ".join(partes[1:3]) if len(partes) > 1 else None
    return marca, modelo


def parse_data_encerramento(raw: str) -> str | None:
    """
    Tenta parsear data do leilão em vários formatos.
    Retorna ISO 8601 com timezone ou None.
    """
    if not raw or raw == "—":
        return None
    # Tenta formatos comuns
    formatos = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    clean = raw.strip()
    for fmt in formatos:
        try:
            dt = datetime.strptime(clean, fmt)
            # Assume horário de Brasília (UTC-3)
            dt_brt = dt.replace(tzinfo=timezone(timedelta(hours=-3)))
            return dt_brt.isoformat()
        except ValueError:
            continue
    return None


# ─── Normalização → schema auctions.veiculos ─────────────────────────────────

def normalize_to_db(lote: dict, tipo_override: str = None) -> dict | None:
    """
    Mapeia os campos do scraper para o schema auctions.veiculos.
    Retorna None se faltarem campos obrigatórios (titulo, valor_inicial, link, ano).
    """
    link   = lote.get("url")
    titulo = lote.get("titulo", "—")

    if not link or not titulo or titulo == "—":
        return None

    valor_inicial = lote.get("lance_raw")
    if not valor_inicial:
        return None

    # Ano
    ano_fabricacao, ano_modelo = parse_ano(lote.get("ano", "—"))
    if not ano_fabricacao:
        # fallback: tenta extrair do titulo
        m = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", titulo)
        if m:
            ano_fabricacao = ano_modelo = int(m.group(1))
        else:
            return None  # NOT NULL no schema

    # Data encerramento (NOT NULL no schema)
    data_enc = None
    raw_data = lote.get("data_encerramento")
    if raw_data:
        data_enc = parse_data_encerramento(raw_data)
    if not data_enc:
        # Fallback: 30 dias no futuro (lote ativo encontrado agora)
        data_enc = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    # Localização
    estado, cidade = parse_localizacao(lote.get("localizacao", "—"))

    # Tipo
    tipo = tipo_override or _tipo_from_url(link)

    # Marca e modelo
    marca, modelo = parse_marca_modelo(titulo)

    # KM
    km = parse_km(lote.get("km"))

    imagens = lote.get("imagens") or []

    # Extrai UUID do lote para usar como namespace no Storage.
    # Garante que imagens de lotes diferentes nunca compartilhem o mesmo path.
    lot_uuid_m = re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", link, re.I
    )
    lot_uuid = lot_uuid_m.group(0).lower() if lot_uuid_m else None

    return {
        "titulo":                titulo,
        "descricao":             lote.get("descricao") if lote.get("descricao") not in (None, "—") else None,
        "tipo":                  tipo,
        "marca":                 marca,
        "modelo":                modelo,
        "estado":                estado,
        "cidade":                cidade,
        "ano_fabricacao":        ano_fabricacao,
        "ano_modelo":            ano_modelo,
        "modalidade":            "leilao",
        "valor_inicial":         valor_inicial,
        "valor_atual":           lote.get("lance_raw"),   # mesmo valor — atualizado em tempo real
        "data_encerramento":     data_enc,
        "link":                  link,
        "imagem_1":              mirror_image(imagens[0] if len(imagens) > 0 else None, lot_uuid=lot_uuid),
        "imagem_2":              mirror_image(imagens[1] if len(imagens) > 1 else None, lot_uuid=lot_uuid),
        "imagem_3":              mirror_image(imagens[2] if len(imagens) > 2 else None, lot_uuid=lot_uuid),
        "percentual_abaixo_fipe": lote.get("desconto_pct"),
        "margem_revenda":         lote.get("margem_revenda"),
        "km":                    km,
        "origem":                lote.get("tipo_retomada") if lote.get("tipo_retomada") not in (None, "—") else "leilo",
        "ativo":                 True,
    }



# ─── Mirror de imagens → Supabase Storage ────────────────────────────────────

import hashlib as _hashlib

LEILO_CDN   = "leilo.cdndp.com.br"
PLACEHOLDER = "/place.png"
BUCKET      = "veiculos-imagens"   # bucket público no Supabase Storage

# Headers que imitam o browser dentro do leilo.com.br
_IMG_HEADERS = {
    "User-Agent":  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 Chrome/122 Safari/537.36",
    "Referer":     "https://leilo.com.br/",
    "Accept":      "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
}

# Hash MD5 da imagem genérica que o leilo serve quando a foto ainda não existe.
# Calculado na primeira chamada e cacheado — qualquer download que bata esse
# hash é descartado e substituído pelo placeholder.
_LEILO_PLACEHOLDER_HASHES: set[str] = set()
_LEILO_PLACEHOLDER_LOADED = False

# URLs CDN conhecidas de imagens-placeholder do leilo (detectadas em produção).
# Adicione aqui novas URLs ruins sempre que surgir um novo placeholder.
_KNOWN_BAD_CDN_URLS: set[str] = {
    "https://leilo.cdndp.com.br/v1/arquivo/2026/2/9/"
    "1770646170608_4c65f0d2-72dc-49d2-a98b-43ef8dc32ae0.jpeg",
}

# Storage paths derivados das URLs ruins acima — bloqueados mesmo após HEAD 200.
# Gerado automaticamente a partir de _KNOWN_BAD_CDN_URLS.
def _bad_storage_paths() -> set[str]:
    paths = set()
    for u in _KNOWN_BAD_CDN_URLS:
        m = re.search(r"/v1/arquivo/(.+)$", u)
        paths.add(m.group(1) if m else u[-60:])
    return paths

_BAD_STORAGE_PATHS: set[str] = _bad_storage_paths()

# Mantém compatibilidade com _KNOWN_BAD_URL usado em _load_bad_hashes
_KNOWN_BAD_URL = next(iter(_KNOWN_BAD_CDN_URLS))

def _load_bad_hashes():
    """Baixa a imagem ruim conhecida e armazena o hash para comparação futura."""
    global _LEILO_PLACEHOLDER_LOADED
    if _LEILO_PLACEHOLDER_LOADED:
        return
    _LEILO_PLACEHOLDER_LOADED = True
    try:
        r = _requests.get(_KNOWN_BAD_URL, headers=_IMG_HEADERS, timeout=10)
        if r.status_code == 200 and r.content:
            h = _hashlib.md5(r.content).hexdigest()
            _LEILO_PLACEHOLDER_HASHES.add(h)
            print(f"  {DIM}📷  hash da imagem-lixo do leilo carregado: {h}{RESET}")
    except Exception as e:
        print(f"  {DIM}⚠️  não conseguiu carregar hash da imagem-lixo: {e}{RESET}")

def _is_bad_image(img_bytes: bytes) -> bool:
    """True se o conteúdo baixado é a imagem genérica do leilo."""
    if not _LEILO_PLACEHOLDER_HASHES:
        return False
    return _hashlib.md5(img_bytes).hexdigest() in _LEILO_PLACEHOLDER_HASHES

def _supabase_storage_url() -> str | None:
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    return f"{base}/storage/v1/object/{BUCKET}" if base else None

def _supabase_public_url(path: str) -> str | None:
    base = os.getenv("SUPABASE_URL", "").rstrip("/")
    return f"{base}/storage/v1/object/public/{BUCKET}/{path}" if base else None

def mirror_image(url: str | None, max_retries: int = 3, lot_uuid: str | None = None) -> str:
    """
    Tenta baixar a imagem do CDN do leilo (com Referer correto) e
    sobe para o Supabase Storage.

    O storage_path é prefixado pelo UUID do lote, garantindo que
    imagens de lotes diferentes nunca compartilhem o mesmo arquivo —
    elimina o bug de imagens cruzadas entre lotes.

    Retorna a URL pública do Storage, ou PLACEHOLDER em caso de falha.
    """
    _load_bad_hashes()   # no-op após primeira chamada

    if not url:
        return PLACEHOLDER

    # Rejeita imediatamente URLs CDN conhecidas como placeholder/lixo.
    if url in _KNOWN_BAD_CDN_URLS:
        print(f"    {DIM}⚠️  URL de imagem-lixo bloqueada: {url[-60:]}{RESET}")
        return PLACEHOLDER

    # Se não é do CDN bloqueado, retorna direta
    if LEILO_CDN not in url:
        return url

    storage_base = _supabase_storage_url()
    if not storage_base:
        return PLACEHOLDER  # sem credenciais, nem tenta

    # Deriva o filename da URL original
    # ex: "1772740577709_abc.jpeg"
    m = re.search(r"/v1/arquivo/(.+)$", url)
    raw_path = m.group(1) if m else re.sub(r"[^a-zA-Z0-9._/-]", "_", url[-60:])
    filename = raw_path.split("/")[-1]  # só o filename

    # Namespace por UUID do lote: "lotes/{lot_uuid}/{filename}"
    # Se não tiver UUID (fallback raro), usa o path original inteiro.
    if lot_uuid:
        storage_path = f"lotes/{lot_uuid}/{filename}"
    else:
        storage_path = raw_path

    # Bloqueia paths que sabemos ser de imagens ruins no Storage
    if raw_path in _BAD_STORAGE_PATHS or storage_path in _BAD_STORAGE_PATHS:
        print(f"    {DIM}⚠️  storage path de imagem-lixo bloqueado: {storage_path}{RESET}")
        return PLACEHOLDER

    public_url = _supabase_public_url(storage_path)

    # Verifica se já existe no storage (evita re-upload).
    # Com namespace por UUID, esse cache é confiável: se existe, é deste lote.
    try:
        check = _requests.head(public_url, timeout=5)
        if check.status_code == 200:
            return public_url
    except Exception:
        pass

    # Tenta baixar com retry
    img_bytes = None
    for attempt in range(1, max_retries + 1):
        try:
            r = _requests.get(url, headers=_IMG_HEADERS, timeout=15)
            if r.status_code == 200 and len(r.content) > 1000:
                img_bytes = r.content
                break
            print(f"    {DIM}⚠️  img HTTP {r.status_code} (tentativa {attempt}/{max_retries}){RESET}")
        except Exception as e:
            print(f"    {DIM}⚠️  img erro: {e} (tentativa {attempt}/{max_retries}){RESET}")
        if attempt < max_retries:
            time.sleep(1.5 * attempt)

    if not img_bytes:
        return PLACEHOLDER

    # Rejeita se for a imagem genérica/placeholder do leilo
    if _is_bad_image(img_bytes):
        print(f"    {DIM}⚠️  imagem genérica do leilo detectada — usando placeholder{RESET}")
        return PLACEHOLDER

    # Detecta content-type
    ct = "image/jpeg"
    if storage_path.endswith(".png"):
        ct = "image/png"
    elif storage_path.endswith(".webp"):
        ct = "image/webp"

    # Sobe para o Supabase Storage
    key  = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    hdrs = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  ct,
        "x-upsert":      "true",
    }
    try:
        up = _requests.post(
            f"{storage_base}/{storage_path}",
            data=img_bytes,
            headers=hdrs,
            timeout=30,
        )
        if up.status_code in (200, 201):
            return public_url
        print(f"    {DIM}⚠️  storage upload HTTP {up.status_code}: {up.text[:120]}{RESET}")
    except Exception as e:
        print(f"    {DIM}⚠️  storage upload erro: {e}{RESET}")

    return PLACEHOLDER

# ─── Passive network capture ─────────────────────────────────────────────────

class NetworkCapture:
    def __init__(self):
        self.responses: list[dict] = []
        self._handler = None

    def attach(self, page):
        async def handler(resp: Response):
            if "leilo.com.br" not in resp.url:
                return
            if "json" not in resp.headers.get("content-type", ""):
                return
            try:
                body = await resp.json()
                self.responses.append({"url": resp.url, "data": body})
            except Exception:
                pass
        self._handler = handler
        page.on("response", self._handler)

    def detach(self, page):
        if self._handler:
            page.remove_listener("response", self._handler)
            self._handler = None

    def reset(self):
        self.responses.clear()


# ─── Extração campos da API ───────────────────────────────────────────────────

_SKIP_SUBSTRINGS = ("id", "uuid", "codigo", "numero", "count", "total",
                    "qtd", "quantidade", "sequence", "posicao", "rank",
                    "historico", "history")


def _key_is_noise(key: str) -> bool:
    kl = key.lower().replace("_", "").replace("-", "")
    return any(s in kl for s in _SKIP_SUBSTRINGS)


def extract_api_fields(responses: list[dict]) -> dict:
    lance_candidates   = []
    mercado_candidates = []
    extras = {}

    def walk(obj, depth=0):
        if depth > 8:
            return
        if isinstance(obj, list):
            for item in obj:
                walk(item, depth + 1)
            return
        if not isinstance(obj, dict):
            return

        for k, v in obj.items():
            kl = k.lower().replace("_", "").replace("-", "")

            is_lance_key = any(x in kl for x in [
                "melhorlance", "lancevalor", "valorlance", "lanceatual",
                "bidvalue", "currentbid", "lastbid", "highestbid",
            ])
            if is_lance_key and not _key_is_noise(k):
                val = parse_brl(v)
                if val:
                    lance_candidates.append((val, k, v))

            is_mercado_key = any(x in kl for x in [
                "valormercado", "valordemercado", "precofipe", "valorefipe",
                "tabelafipe", "fipevalor", "marketvalue", "marketprice",
            ])
            if is_mercado_key and not _key_is_noise(k):
                val = parse_brl(v)
                if val:
                    mercado_candidates.append((val, k, v))

            if kl in ("km", "quilometragem", "odometro", "kilometragem") and not extras.get("km"):
                try:
                    extras["km"] = int(float(str(v).replace(".", "").replace(",", ".")))
                except Exception:
                    pass

            if kl in ("cor", "cordo", "corveiculo") and isinstance(v, str) and 2 < len(v) < 30:
                extras.setdefault("cor", v)

            if ("combustivel" in kl or "fuel" in kl) and isinstance(v, str) and len(v) < 30:
                extras.setdefault("combustivel", v)

            if kl == "retomada" and isinstance(v, str) and 2 < len(v) < 80:
                extras.setdefault("retomada", v)

            if kl in ("anomodelo", "anofabricacao", "modelyear") and isinstance(v, (int, str)):
                extras.setdefault("ano", str(v))

            if "placa" in kl and isinstance(v, str) and 5 < len(v) < 12:
                extras.setdefault("placa", v)

            # Data encerramento via API
            if any(x in kl for x in ["dataencerramento", "datafim", "enddate", "dtfim",
                                       "dataauction", "encerramento"]):
                if isinstance(v, str) and len(v) > 6:
                    extras.setdefault("data_encerramento_api", v)

            # Imagens via fotosUrls (campo principal da API do leilo).
            # Coleta TODOS os candidatos e usa o MAIOR conjunto encontrado —
            # evita que uma resposta de listagem (fotos de outros lotes)
            # sobrescreva as fotos corretas do lote atual.
            if kl in ("fotosurls", "fotos", "fotosurl", "photos", "images", "imagens"):
                if isinstance(v, list):
                    urls_validas = [
                        u for u in v
                        if isinstance(u, str) and u.startswith("http")
                        and "placeholder" not in u and "logo" not in u
                    ]
                    if urls_validas:
                        current = extras.get("fotos_api") or []
                        if len(urls_validas) > len(current):
                            extras["fotos_api"] = urls_validas

            walk(v, depth + 1)

    for cap in responses:
        walk(cap["data"])

    result = dict(extras)

    if lance_candidates:
        lance_candidates.sort(key=lambda x: x[0], reverse=True)
        result["lance"] = lance_candidates[0][2]

    if mercado_candidates:
        mercado_candidates.sort(key=lambda x: x[0], reverse=True)
        result["valor_mercado"] = mercado_candidates[0][2]

    return result


# ─── DOM JS (inclui imagens e data de encerramento) ───────────────────────────

DOM_JS = """
() => {
    const out = { pares: {}, rValues: [], imagens: [] };

    // Título
    out.titulo = (
        document.querySelector('h1')?.innerText ||
        document.querySelector('[class*="titulo-veiculo"]')?.innerText || ''
    ).trim().replace(/\\s+/g, ' ');

    // Pares label → valor
    document.querySelectorAll('[class*="label-categoria"]').forEach(label => {
        const col = label.closest('[class*="col-"]');
        if (!col) return;
        const val = col.querySelector('[class*="text-weight-semibold"], p');
        const k = label.innerText?.replace(/\\s+/g, ' ').trim();
        const v = val?.innerText?.replace(/\\s+/g, ' ').trim();
        if (k && v && k !== v && v.length < 200) out.pares[k] = v;
    });

    // Valores monetários visíveis
    const seen = new Set();
    document.querySelectorAll('*').forEach(el => {
        if (el.children.length > 0) return;
        const t = (el.innerText || '').trim();
        if (!t || seen.has(t)) return;
        seen.add(t);
        if (/R\\$\\s*[\\d,.]+/.test(t)) {
            out.rValues.push({ text: t, cls: typeof el.className === 'string' ? el.className : '' });
        }
    });

    // Lance atual
    const lanceSelectors = [
        '[class*="lance-atual"]','[class*="lanceAtual"]','[class*="melhor-lance"]',
        '[class*="melhorLance"]','[class*="valor-lance"]','[class*="bid"]',
        '[id*="lance"]','[data-cy*="lance"]'
    ];
    for (const s of lanceSelectors) {
        const el = document.querySelector(s);
        if (el && /R\\$/.test(el.innerText)) { out.lance_dom = el.innerText.trim(); break; }
    }

    // Data de encerramento
    const dataSelectors = [
        '[class*="encerramento"]','[class*="data-fim"]','[class*="countdown"]',
        '[class*="timer"]','[data-cy*="encerramento"]','[class*="prazo"]',
        '[class*="data-leilao"]','[class*="auction-date"]'
    ];
    for (const s of dataSelectors) {
        const el = document.querySelector(s);
        if (el) {
            const t = el.getAttribute('data-date') || el.getAttribute('datetime') || el.innerText;
            if (t && t.trim().length > 4) { out.data_encerramento = t.trim(); break; }
        }
    }

    // Imagens do veículo
    const imgSelectors = [
        '[class*="foto-veiculo"] img',
        '[class*="galeria"] img',
        '[class*="carousel"] img',
        '[class*="slider"] img',
        '[class*="vehicle-image"] img',
        '.q-carousel img',
    ];
    const imgSeen = new Set();
    for (const s of imgSelectors) {
        document.querySelectorAll(s).forEach(img => {
            const src = img.src || img.dataset.src || '';
            if (src && !imgSeen.has(src) && !src.includes('placeholder')
                     && !src.includes('logo') && src.startsWith('http')) {
                imgSeen.add(src);
                out.imagens.push(src);
            }
        });
        if (out.imagens.length >= 3) break;
    }

    // Lote
    out.num_lote = (
        document.querySelector('[class*="num-lote"]')?.innerText ||
        document.querySelector('[class*="numero-lote"]')?.innerText || ''
    ).trim();

    // Descrição
    out.descricao = (
        document.querySelector('[class*="descricao-lote"]')?.innerText ||
        document.querySelector('[class*="descricao"]')?.innerText || ''
    ).trim().slice(0, 500);

    return out;
}
"""

# ─── Listagem (paginação Quasar) ──────────────────────────────────────────────

async def _get_uuid_links(page) -> list[str]:
    return await page.evaluate("""
        () => {
            const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
            const seen = new Set(); const out = [];
            document.querySelectorAll('a[href]').forEach(a => {
                if (UUID.test(a.href) && !seen.has(a.href)) {
                    seen.add(a.href); out.push(a.href);
                }
            });
            return out;
        }
    """)


async def _next_btn_enabled(page) -> bool:
    return await page.evaluate("""
        () => {
            const pg = document.querySelector('.q-pagination');
            if (!pg) return false;
            const btns = Array.from(pg.querySelectorAll('button'));
            const nav = btns.filter(b => !/^[0-9]+$/.test(b.textContent.trim()));
            const next = nav[nav.length - 1];
            return !!(next && !next.disabled && !next.hasAttribute('disabled')
                      && next.getAttribute('aria-disabled') !== 'true');
        }
    """)


async def _click_page_btn(page, pg_num: int) -> bool:
    return await page.evaluate(
        "(n) => {"
        "  const btns = document.querySelectorAll('.q-pagination button[aria-label]');"
        "  for (const b of btns) {"
        "    if (parseInt(b.getAttribute('aria-label'),10) === n && !b.disabled) {"
        "      b.click(); return true;"
        "    }"
        "  }"
        "  return false;"
        "}",
        pg_num,
    )


async def _click_next_arrow(page) -> bool:
    return await page.evaluate("""
        () => {
            const pg = document.querySelector('.q-pagination');
            if (!pg) return false;
            const btns = Array.from(pg.querySelectorAll('button'));
            const nav = btns.filter(b => !/^[0-9]+$/.test(b.textContent.trim()));
            const next = nav[nav.length - 1];
            if (next && !next.disabled && !next.hasAttribute('disabled')
                && next.getAttribute('aria-disabled') !== 'true') {
                next.click(); return true;
            }
            return false;
        }
    """)


async def _wait_for_page(page, pg_num: int):
    try:
        await page.wait_for_function(
            "(n) => {"
            "  const all = document.querySelectorAll('.q-pagination button[aria-label]');"
            "  for (const btn of all) {"
            "    if (parseInt(btn.getAttribute('aria-label'),10) === n"
            "        && btn.getAttribute('aria-current') === 'true') return true;"
            "  }"
            "  return false;"
            "}",
            arg=pg_num,
            timeout=8000,
        )
    except Exception:
        await asyncio.sleep(1.5)
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(0.6)


async def _collect_page_links(page, seen: set, all_links: list) -> int:
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(0.4)
    for _ in range(4):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(0.35)
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(0.3)

    links = await _get_uuid_links(page)
    new_count = 0
    for lnk in links:
        if lnk not in seen:
            seen.add(lnk)
            all_links.append(lnk)
            new_count += 1
    return new_count


async def get_lot_links(page, url: str) -> list[str]:
    print(f"\n{CYAN}📡 Abrindo listagem...{RESET}")
    await page.goto(url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(1.0)

    all_links: list[str] = []
    seen: set[str] = set()
    pg_num = 1

    while True:
        print(f"  {CYAN}📄 Coletando página {pg_num}...{RESET}")
        new_count = await _collect_page_links(page, seen, all_links)
        print(f"  {DIM}  +{new_count} novos links (total: {len(all_links)}){RESET}")

        if not await _next_btn_enabled(page):
            print(f"  {DIM}Última página ({pg_num}).{RESET}")
            break

        pg_num += 1
        clicked = await _click_page_btn(page, pg_num)
        if not clicked:
            await _click_next_arrow(page)
            await asyncio.sleep(0.8)
            await page.wait_for_load_state("networkidle")
            clicked = await _click_page_btn(page, pg_num)

        if not clicked:
            print(f"  {DIM}Navegando via seta para pág {pg_num}...{RESET}")

        await _wait_for_page(page, pg_num)

    return all_links


# ─── Detalhe do lote ──────────────────────────────────────────────────────────

async def get_lot_detail(page, url: str, net: NetworkCapture,
                         debug: bool = False) -> dict:
    net.reset()
    net.attach(page)

    await page.goto(url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(1.2)

    dom = await page.evaluate(DOM_JS)

    # Extrai o UUID do lote a partir da URL para filtrar respostas de rede.
    # Evita que respostas de listagem/recomendações (com fotos de OUTROS lotes)
    # contaminem as fotos do lote atual.
    lot_uuid_match = re.search(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", url, re.I
    )
    lot_uuid = lot_uuid_match.group(0).lower() if lot_uuid_match else None

    # Prioridade: respostas cujo URL contém o UUID do lote atual.
    # Fallback: todas as respostas (comportamento anterior).
    if lot_uuid:
        specific = [r for r in net.responses if lot_uuid in r["url"].lower()]
        responses_to_use = specific if specific else net.responses
    else:
        responses_to_use = net.responses

    api = extract_api_fields(responses_to_use)

    if debug:
        print(f"\n  {DIM}── DEBUG ─────────────────────────────────────────{RESET}")
        print(f"  {DIM}API responses total: {len(net.responses)} | usadas: {len(responses_to_use)}{RESET}")
        for r in responses_to_use[:3]:
            print(f"  {DIM}  {r['url']}")
            print(f"       {json.dumps(r['data'], ensure_ascii=False)[:300]}{RESET}")
        print(f"  {DIM}API extraído: {api}{RESET}")
        print(f"  {DIM}DOM pares:    {dom.get('pares')}{RESET}")
        print()

    net.detach(page)
    pares = dom.get("pares") or {}

    # Lance
    lance_raw = api.get("lance") or dom.get("lance_dom")
    if not lance_raw:
        vals = sorted([parse_brl(r["text"]) for r in dom.get("rValues", [])
                       if parse_brl(r["text"])])
        if len(vals) >= 2:
            lance_raw = vals[0]

    # Mercado
    mercado_pares = parse_brl(pares.get("Valor Mercado"))
    mercado_api   = parse_brl(api.get("valor_mercado"))
    lance_f = parse_brl(lance_raw)

    def mercado_ok(m):
        if not m: return False
        if lance_f and m < lance_f: return False
        if lance_f and m > lance_f * 20: return False
        return True

    if mercado_pares and mercado_ok(mercado_pares):
        mercado_raw = mercado_pares
    elif mercado_api and mercado_ok(mercado_api):
        mercado_raw = mercado_api
    else:
        vals = sorted(
            [parse_brl(r["text"]) for r in dom.get("rValues", [])
             if mercado_ok(parse_brl(r["text"]))],
            reverse=True,
        )
        mercado_raw = vals[0] if vals else (mercado_pares or mercado_api)

    def pares_or_api(pares_key, api_key=None):
        v = pares.get(pares_key)
        if v and v != "—":
            v = re.sub(r'\s*(info_outline|info|warning|error)\s*$', '',
                       v, flags=re.I).strip()
            return v or "—"
        return (api.get(api_key) if api_key else None) or "—"

    desc_pct = pct_desconto(lance_raw, mercado_raw)

    # Data encerramento: DOM → API → None
    data_enc = dom.get("data_encerramento") or api.get("data_encerramento_api")

    # Imagens: fotosUrls da API é a fonte primária; DOM como fallback
    imagens = api.get("fotos_api") or dom.get("imagens") or []

    # Margem de revenda: FIPE - lance - 15k (custo de regularização/lucro mínimo)
    margem_revenda = None
    m_fipe  = parse_brl(mercado_raw)
    m_lance = parse_brl(lance_raw)
    if m_fipe and m_lance:
        margem = round(m_fipe - m_lance - 15_000, 2)
        margem_revenda = margem if margem >= 10_000 else None

    return {
        "url":               url,
        "titulo":            dom.get("titulo") or "—",
        "lance":             fmt_brl(lance_raw) if lance_raw else "—",
        "lance_raw":         parse_brl(lance_raw),
        "valor_mercado":     fmt_brl(mercado_raw) if mercado_raw else "—",
        "valor_mercado_raw": parse_brl(mercado_raw),
        "desconto_pct":      desc_pct,
        "desconto_label":    f"{desc_pct}% abaixo do mercado" if desc_pct is not None else "—",
        "margem_revenda":    margem_revenda,
        "ano":               pares_or_api("Ano", "ano"),
        "km":                pares_or_api("Km", "km"),
        "combustivel":       pares_or_api("Combustivel", "combustivel"),
        "cor":               pares_or_api("Cor", "cor"),
        "chave":             pares_or_api("Possui Chave"),
        "tipo_retomada":     pares_or_api("Tipo Retomada", "retomada"),
        "localizacao":       pares_or_api("Localização"),
        "descricao":         dom.get("descricao") or "—",
        "imagens":           imagens,
        "data_encerramento": data_enc,
    }


# ─── Upload para Supabase ─────────────────────────────────────────────────────

def upload_to_supabase(lotes: list[dict], tipo: str) -> dict:
    """Normaliza e faz upsert dos lotes em auctions.veiculos."""
    db = SupabaseClient()

    registros = []
    skipped = 0

    for lote in lotes:
        rec = normalize_to_db(lote, tipo_override=tipo)
        if rec:
            registros.append(rec)
        else:
            skipped += 1

    if skipped:
        print(f"  {YELLOW}⚠️  {skipped} lote(s) ignorado(s) (campos obrigatórios ausentes){RESET}")

    if not registros:
        print(f"  {RED}Nenhum registro válido para upload.{RESET}")
        return {}

    print(f"\n{BOLD}{'═'*56}{RESET}")
    print(f"{BOLD}  ☁️   UPLOAD → auctions.veiculos  ({len(registros)} registros){RESET}")
    print(f"{BOLD}{'═'*56}{RESET}\n")

    stats = db.upsert_veiculos(registros)

    total_enviados = stats.get('inserted', 0) + stats.get('updated', 0)
    print(f"\n  ✅  Enviados:         {total_enviados}  ({stats.get('inserted', 0)} novos  +  {stats.get('updated', 0)} atualizados)")
    print(f"  🔄  Dupes removidas:  {stats.get('duplicates_removed', 0)}")

    return stats


# ─── Main ─────────────────────────────────────────────────────────────────────

async def scrape_categoria(url: str, tipo: str, ctx, args) -> list[dict]:
    """Scrapa uma categoria e retorna os lotes coletados."""
    net = NetworkCapture()
    page_list = await ctx.new_page()
    lot_links = await get_lot_links(page_list, url)
    lot_links = list(dict.fromkeys(lot_links))[:args.max]
    await page_list.close()

    print(f"{GREEN}  ✅ {len(lot_links)} lotes encontrados em {url}{RESET}\n")

    if not lot_links:
        print(f"{RED}  ⚠️  Nenhum lote. Tente --show (pode ter captcha).{RESET}\n")
        return []

    if args.no_details:
        return [{"url": lnk} for lnk in lot_links]

    page_det = await ctx.new_page()
    lotes   = []
    lixo    = 0   # margem < 10k
    sem_lance = 0

    MAX_TENTATIVAS = 3

    def _lote_ok(lote: dict) -> tuple[bool, str]:
        """Verifica se o lote tem todos os campos obrigatórios."""
        if not lote.get("lance_raw"):
            return False, "sem lance"
        if not lote.get("valor_mercado_raw"):
            return False, "sem FIPE"
        if lote.get("titulo") in (None, "—", ""):
            return False, "sem título"
        imagens = lote.get("imagens") or []
        if not imagens:
            return False, "sem imagens"
        return True, "ok"

    for i, link in enumerate(lot_links, 1):
        prefix = f"  {DIM}[{i:>3}/{len(lot_links)}]{RESET}"
        print(f"{prefix} ...{link[-50:]}")

        lote = None
        for tentativa in range(1, MAX_TENTATIVAS + 1):
            try:
                lote = await get_lot_detail(page_det, link, net, debug=args.debug)
                ok, motivo = _lote_ok(lote)
                if ok:
                    break
                # sem lance ativo é definitivo — não retenta
                if motivo == "sem lance":
                    lote = None
                    break
                print(f"         {DIM}↻  tentativa {tentativa}/{MAX_TENTATIVAS}: {motivo}{RESET}")
                await asyncio.sleep(2 * tentativa)
            except Exception as e:
                print(f"         {DIM}↻  tentativa {tentativa}/{MAX_TENTATIVAS} ERRO: {e}{RESET}")
                lote = None
                await asyncio.sleep(2 * tentativa)

        if lote is None:
            sem_lance += 1
            print(f"         {DIM}⏭  sem lance ativo — ignorado{RESET}\n")
            continue

        ok, motivo = _lote_ok(lote)
        if not ok:
            print(f"         {RED}✗  descartado após {MAX_TENTATIVAS} tentativas: {motivo}{RESET}\n")
            continue

        # Filtra margem < 10k — isso fede
        margem = lote.get("margem_revenda")
        if margem is None:
            lixo += 1
            titulo_lixo = (lote.get("titulo") or "")[:45]
            print(f"         {DIM}🗑  margem insuficiente — {titulo_lixo}{RESET}\n")
            continue

        lotes.append(lote)
        titulo = (lote["titulo"] or "")[:50]
        desc   = lote.get("desconto_pct")
        n_imgs = len(lote.get("imagens") or [])
        cor    = GREEN if (desc or 0) >= 30 else YELLOW
        print(f"         {YELLOW}{titulo}{RESET}")
        print(f"         Lance {GREEN}{lote['lance']}{RESET}  ·  "
              f"Mercado {lote['valor_mercado']}  ·  "
              f"{cor}{lote['desconto_label']}{RESET}"
              f"  ·  Margem {GREEN}R$ {margem:,.0f}{RESET}"
              f"  ·  🖼 {n_imgs} fotos\n")

    if sem_lance:
        print(f"  {DIM}⏭  {sem_lance} lote(s) sem lance ativo{RESET}\n")
    if lixo:
        print(f"  {DIM}🗑  {lixo} lote(s) descartados (margem < R$ 10.000){RESET}\n")

    await page_det.close()
    return lotes


async def main():
    parser = argparse.ArgumentParser(description="Leilo scraper → auctions.veiculos")
    parser.add_argument("--url", help="URL de uma categoria específica")
    parser.add_argument("--all", action="store_true",
                        help="Scrapa todas as categorias de veículos")
    parser.add_argument("--output", "-o", default="lotes.json")
    parser.add_argument("--no-details", action="store_true", help="Só links")
    parser.add_argument("--no-upload", action="store_true",
                        help="Não envia para o Supabase (debug local)")
    parser.add_argument("--show", action="store_true", help="Janela do browser")
    parser.add_argument("--debug", action="store_true", help="Dump das respostas API")
    parser.add_argument("--max", type=int, default=999, help="Máx de lotes por categoria")
    args = parser.parse_args()

    if not args.url and not args.all:
        parser.error("Informe --url <URL> ou use --all para todas as categorias.")

    # Monta lista de (url, tipo)
    if args.all:
        targets = [(url, tipo) for _, (url, tipo) in CATEGORIAS.items()]
    else:
        targets = [(args.url, _tipo_from_url(args.url))]

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  🚗  LEILO SCRAPER → auctions.veiculos{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  {DIM}{len(targets)} categoria(s) | upload: {'não' if args.no_upload else 'sim'}{RESET}\n")

    todos_lotes: list[dict] = []
    stats_total = {'inserted': 0, 'updated': 0, 'errors': 0, 'duplicates_removed': 0}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.show)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/122 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="pt-BR",
        )

        for cat_url, tipo in targets:
            print(f"\n{BOLD}{'─'*64}{RESET}")
            print(f"{BOLD}  📂  Categoria: {tipo.upper()} — {cat_url}{RESET}")
            print(f"{BOLD}{'─'*64}{RESET}")

            lotes = await scrape_categoria(cat_url, tipo, ctx, args)

            if lotes:
                todos_lotes.extend(lotes)

                if not args.no_upload:
                    stats = upload_to_supabase(lotes, tipo)
                    for k in ('inserted', 'updated', 'errors', 'duplicates_removed'):
                        stats_total[k] = stats_total.get(k, 0) + stats.get(k, 0)

        await browser.close()

    # Salva JSON local
    com = sorted([l for l in todos_lotes if isinstance(l.get("desconto_pct"), float)],
                 key=lambda x: x["desconto_pct"], reverse=True)
    sem = [l for l in todos_lotes if not isinstance(l.get("desconto_pct"), float)]

    output = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_lotes": len(todos_lotes),
        "com_desconto": len(com),
        "lotes": com + sem,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Resumo final
    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  📊  RESUMO FINAL{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  Total lotes coletados: {len(todos_lotes)}")
    print(f"  Com desconto calculado: {len(com)}")
    if not args.no_upload:
        total_s = stats_total['inserted'] + stats_total['updated']
        print(f"  Enviados ao Supabase:   {total_s}  ({stats_total['inserted']} novos  +  {stats_total['updated']} atualizados)")
        print(f"  Erros:                  {stats_total['errors']}")


if __name__ == "__main__":
    asyncio.run(main())