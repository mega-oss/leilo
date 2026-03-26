#!/usr/bin/env python3
"""
leilo_gpu.py — Extrator de Placas de Vídeo do leilo.com.br → auctions.tecnologia

Busca lotes na categoria Equipamentos, detecta placas de vídeo pelo título,
busca o preço de mercado via scraping do Mercado Livre (Playwright) e
faz upsert na tabela auctions.tecnologia do Supabase.

Uso:
    python leilo_gpu.py                   # extrai, compara preços e sobe pro Supabase
    python leilo_gpu.py --no-upload       # debug local sem subir
    python leilo_gpu.py --no-market       # pula busca de preço de mercado
    python leilo_gpu.py --output out.json # arquivo de saída customizado
    python leilo_gpu.py --max 50          # limita resultados
    python leilo_gpu.py --debug           # mostra respostas brutas da API
    python leilo_gpu.py --show            # browser visível pra cookies
"""

import asyncio
import httpx
import json
import re
import sys
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from playwright.async_api import async_playwright

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

# ─── Config ───────────────────────────────────────────────────────────────────
BASE_URL = "https://leilo.com.br/leilao/tecnologia"
API_URL  = "https://api.leilo.com.br/v1/lote/busca-elastic"

API_HEADERS = {
    "accept":           "application/json, text/plain, */*",
    "accept-language":  "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type":     "application/json",
    "origin":           "https://leilo.com.br",
    "referer":          "https://leilo.com.br/",
    "user-agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.5 Mobile/15E148 Safari/604.1"
    ),
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

# ─── Detecção de GPU ──────────────────────────────────────────────────────────
GPU_EXCLUDE = re.compile(
    r"""
      \bparalama\b
    | \btampa\s+lateral\b
    | \btampa\s+traseira\b
    | \bfanal\b
    | \bcarenagem\b
    | \bpara.barro\b
    | \b(yamaha|honda|suzuki|kawasaki|bmw|ducati)\b
    | \bmoto\b(?!\s*(placa|nvidia|amd|geforce|radeon|rtx|gtx))
    | \bauto\s*pe[cç]as?\b
    | \bpneu\b
    | \bretrovi[sz]or\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

GPU_CONFIRM = re.compile(
    r"""
      \brtx\s*\d{3,4}\b
    | \bgtx\s*[789]\d{2}\b
    | \bgtx\s*1\d{3}\b
    | \bgt\s*\d{3,4}\b
    | \brx\s*[456789]\d{2,3}\b
    | \bvega\s*\d+\b
    | \bradeon\s+rx\b
    | \bgeforce\b
    | \bplaca\s+de\s+v[ií]deo\b
    | \bplaca\s+vga\b
    | \bvram\b
    | \bgddr[3-6x]+\b
    | \bquadro\s+[a-z]?\d+\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_NUM_FLOOR_RE = re.compile(r"\b(gtx|rtx)\s*(\d{3,4})\b", re.IGNORECASE)


def is_gpu(titulo: str) -> bool:
    if GPU_EXCLUDE.search(titulo):
        return False
    if not GPU_CONFIRM.search(titulo):
        return False
    for m in _NUM_FLOOR_RE.finditer(titulo):
        if int(m.group(2)) < 400:
            return False
    return True


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
        if 50 <= val <= 500_000:
            return val
    except Exception:
        pass
    return None


def fmt_brl(v) -> str:
    val = parse_brl(v)
    if val is None:
        return str(v) if v else "—"
    s = f"{val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def pct_desconto(lance, mercado) -> float | None:
    l, m = parse_brl(lance), parse_brl(mercado)
    if l and m and m > 0:
        return round((1 - l / m) * 100, 1)
    return None


# ─── Parse de data ────────────────────────────────────────────────────────────

def parse_data(raw) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    try:
        ts = float(s)
        if ts > 1e10:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    except Exception:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).isoformat()
    except Exception:
        pass
    for fmt in ["%d/%m/%Y %H:%M", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone(timedelta(hours=-3))).isoformat()
        except ValueError:
            continue
    return None


# ─── Extração do modelo e marca ───────────────────────────────────────────────

def extrair_modelo_gpu(titulo: str) -> str:
    t = titulo.upper()

    m = re.search(r"\b(RTX|GTX|GT|RX)\s*(\d{3,4})\s*(TI|XT|SUPER|M\b)?", t)
    if m:
        parts = [m.group(1), m.group(2)]
        if m.group(3):
            parts.append(m.group(3).strip())
        return " ".join(parts)

    m = re.search(r"PLACA\s+DE\s+V[IÍ]DEO\s+(\d{4})\b", t)
    if m:
        num = m.group(1)
        prefix = "RTX" if num.startswith(("20", "30", "40")) else "GTX"
        return f"{prefix} {num}"

    m = re.search(r"\b(QUADRO|TITAN\s+RTX)\s+(\w+)\b", t)
    if m:
        return f"{m.group(1)} {m.group(2)}"

    m = re.search(r"\bRADEON\s+(RX\s*\d+\w*|R\d\s*\d+\w*|VEGA\s*\d+)", t)
    if m:
        return f"Radeon {m.group(1)}"

    m = re.search(r"\bGEFORCE\s+(\w+(?:\s+\w+)?)\b", t)
    if m:
        return f"GeForce {m.group(1)}"

    palavras = re.sub(r"[^\w\s]", " ", titulo).split()
    return " ".join(palavras[:4])


def extrair_marca_gpu(titulo: str) -> str | None:
    t = titulo.upper()
    for marca in ["NVIDIA", "AMD", "ASUS", "GIGABYTE", "MSI", "ZOTAC",
                  "SAPPHIRE", "POWERCOLOR", "EVGA", "PALIT", "XFX", "GALAX"]:
        if marca in t:
            return marca.title()
    if re.search(r"\b(RTX|GTX|GT|GEFORCE|QUADRO)\b", t):
        return "Nvidia"
    if re.search(r"\b(RX\s*\d|RADEON|VEGA)\b", t):
        return "AMD"
    return None


def tags_oportunidade(desconto_pct: float | None, margem: float | None) -> list:
    tags = []
    if desconto_pct is not None:
        if desconto_pct >= 60:
            tags.append("super_oferta")
        elif desconto_pct >= 40:
            tags.append("boa_oferta")
        elif desconto_pct >= 20:
            tags.append("oportunidade")
    if margem is not None and margem >= 500:
        tags.append("margem_alta")
    return tags


# ─── Normalização → schema auctions.tecnologia ───────────────────────────────

def normalize_to_db(gpu: dict) -> dict | None:
    link   = gpu.get("url")
    titulo = gpu.get("titulo", "")
    if not link or not titulo:
        return None

    lance = gpu.get("lance_raw")
    if not lance:
        return None

    data_enc = parse_data(gpu.get("data_encerramento"))
    if not data_enc:
        data_enc = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    # Localização
    loc    = gpu.get("localizacao") or ""
    estado, cidade = None, None
    m = re.search(r"(.+?)\s*[-/]\s*([A-Z]{2})\s*$", loc.strip())
    if m:
        cidade = m.group(1).strip()
        estado = m.group(2).upper()
    else:
        m2 = re.search(r"\b([A-Z]{2})\b", loc)
        if m2:
            estado = m2.group(1)

    imagens  = gpu.get("imagens") or []
    modelo   = gpu.get("modelo_gpu", "")
    marca    = extrair_marca_gpu(titulo)

    mercado  = gpu.get("preco_mercado") or {}
    pm       = mercado.get("preco_medio")
    pm_min   = mercado.get("preco_min")
    pm_max   = mercado.get("preco_max")

    custo    = gpu.get("total_a_pagar_raw") or lance
    desc_pct = gpu.get("desconto_pct")
    margem   = gpu.get("margem_bruta")

    # Especificações detectadas do título
    especificacoes: dict = {"modelo_completo": titulo}
    m_vram = re.search(r"(\d+)\s*GB", titulo, re.IGNORECASE)
    if m_vram:
        especificacoes["vram_gb"] = int(m_vram.group(1))
    m_gddr = re.search(r"(GDDR\w+)", titulo, re.IGNORECASE)
    if m_gddr:
        especificacoes["memoria_tipo"] = m_gddr.group(1).upper()
    if mercado.get("url_busca"):
        especificacoes["url_busca_mercado"] = mercado["url_busca"]

    return {
        "titulo":                    titulo,
        "descricao":                 gpu.get("descricao") or None,
        "tipo":                      "informatica",
        "sub_categoria":             "placa_de_video",
        "marca":                     marca,
        "modelo":                    modelo,
        "especificacoes":            especificacoes,
        "estado":                    estado,
        "cidade":                    cidade,
        "modalidade":                "leilao",
        "valor_inicial":             lance,
        "valor_atual":               custo,
        "data_encerramento":         data_enc,
        "link":                      link,
        "imagem_1":                  imagens[0] if len(imagens) > 0 else None,
        "imagem_2":                  imagens[1] if len(imagens) > 1 else None,
        "imagem_3":                  imagens[2] if len(imagens) > 2 else None,
        "preco_mercado":             pm,
        "preco_mercado_min":         pm_min,
        "preco_mercado_max":         pm_max,
        "percentual_abaixo_mercado": desc_pct,
        "margem_revenda":            margem,
        "alta_procura":              desc_pct is not None and desc_pct >= 40,
        "tags_oportunidade":         tags_oportunidade(desc_pct, margem),
        "destaque":                  desc_pct is not None and desc_pct >= 50,
        "ativo":                     True,
        "origem":                    "leilo",
    }


# ─── Busca de preço via Playwright (Mercado Livre scraping) ──────────────────

async def _scrape_ml_precos(modelo: str, debug: bool = False) -> list:
    query = f"placa de video {modelo}"
    url   = (
        "https://lista.mercadolivre.com.br/"
        + query.replace(" ", "-")
        + "_OrderId_PRICE_NoIndex_True"
    )
    precos = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="pt-BR",
            viewport={"width": 1280, "height": 900},
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        )
        page = await ctx.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            await asyncio.sleep(2)

            seletores = [
                "span.andes-money-amount__fraction",
                ".price-tag-fraction",
                "[class*='price'] [class*='fraction']",
            ]

            textos = []
            for sel in seletores:
                elementos = await page.query_selector_all(sel)
                if elementos:
                    for el in elementos:
                        t = await el.inner_text()
                        textos.append(t)
                    if debug:
                        print(f"  {DIM}[ML scrape] seletor={sel!r} → {len(textos)} valores{RESET}")
                    break

            if not textos:
                html = await page.content()
                textos = re.findall(r'"price"\s*:\s*(\d+(?:\.\d+)?)', html)
                if debug:
                    print(f"  {DIM}[ML scrape] fallback JSON-in-HTML → {len(textos)} matches{RESET}")

        except Exception as e:
            if debug:
                print(f"  {RED}[ML scrape] erro playwright: {e}{RESET}")
            textos = []
        finally:
            await browser.close()

    for t in textos:
        s = str(t).replace(".", "").replace(",", "").strip()
        try:
            v = float(s)
            if 200 <= v <= 25_000:
                precos.append(v)
        except ValueError:
            pass

    return precos


async def buscar_preco_mercadolivre(modelo: str, debug: bool = False) -> dict:
    query     = f"placa de video {modelo}"
    url_busca = (
        "https://lista.mercadolivre.com.br/"
        + query.replace(" ", "-")
        + "_OrderId_PRICE_NoIndex_True"
    )

    todos_precos = await _scrape_ml_precos(modelo, debug=debug)

    if not todos_precos:
        return {
            "preco_medio":    None,
            "preco_min":      None,
            "preco_max":      None,
            "num_resultados": 0,
            "fonte":          "mercadolivre_scrape",
            "modelo_buscado": modelo,
            "url_busca":      url_busca,
        }

    todos_precos.sort()
    mediana   = todos_precos[len(todos_precos) // 2]
    filtrados = [p for p in todos_precos if mediana * 0.25 <= p <= mediana * 3.5]
    if not filtrados:
        filtrados = todos_precos

    preco_medio = round(sum(filtrados) / len(filtrados), 2)

    return {
        "preco_medio":    preco_medio,
        "preco_min":      min(filtrados),
        "preco_max":      max(filtrados),
        "num_resultados": len(filtrados),
        "fonte":          "mercadolivre_scrape",
        "modelo_buscado": modelo,
        "url_busca":      url_busca,
    }


# ─── Cookies via Playwright ───────────────────────────────────────────────────

async def get_session_cookies(show: bool = False) -> dict:
    print(f"  {DIM}Obtendo cookies de sessão via browser...{RESET}")
    cookies = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not show,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--window-size=390,844",
            ],
        )
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                "Version/18.5 Mobile/15E148 Safari/604.1"
            ),
            viewport={"width": 390, "height": 844},
            locale="pt-BR",
            bypass_csp=True,
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]});"
            "Object.defineProperty(navigator,'languages',{get:()=>['pt-BR','pt','en']});"
        )
        page = await ctx.new_page()
        try:
            await page.goto(BASE_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
        except Exception:
            pass
        raw = await ctx.cookies()
        cookies = {c["name"]: c["value"] for c in raw}
        await browser.close()
    print(f"  {GREEN}✓  {len(cookies)} cookies obtidos{RESET}")
    return cookies


# ─── Payload da API ───────────────────────────────────────────────────────────

def _build_payload(offset: int, size: int) -> dict:
    return {
        "from": offset,
        "size": size,
        "requisicoesBusca": [
            {"campo": "tipo", "tipo": "exata", "label": "Tipo", "valor": "Equipamentos"}
        ],
        "listaOrdenacao": [
            {"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}
        ],
    }


_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)


def _slugify(s: str) -> str:
    s = str(s).lower().strip()
    for src, dst in [("àáâãä","a"),("èéêë","e"),("ìíîï","i"),("òóôõö","o"),("ùúûü","u"),("ç","c"),("ñ","n")]:
        for c in src:
            s = s.replace(c, dst)
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def _extract_item(item: dict) -> dict | None:
    uuid = item.get("id") or ""
    if not uuid:
        m = _UUID_RE.search(str(item))
        uuid = m.group(0).lower() if m else None
    if not uuid:
        return None

    veiculo = item.get("veiculo") or {}
    produto = item.get("produto") or {}
    valor   = item.get("valor") or {}
    lance_d = valor.get("lance") or {}

    titulo = (
        item.get("nome") or item.get("tituloProduto") or
        item.get("nomeProduto") or produto.get("nome") or
        produto.get("titulo") or ""
    ).strip()

    if not titulo or not is_gpu(titulo):
        return None

    lance_raw = None
    for v in [lance_d.get("valor"), valor.get("valorProposta"), valor.get("minimo")]:
        if v is not None:
            lance_raw = parse_brl(v)
            if lance_raw:
                break
    if lance_raw and lance_raw < 50:
        lance_raw = None

    total_a_pagar = None
    v = valor.get("totalAPagar")
    if v is not None:
        try:
            total_a_pagar = float(v)
            if total_a_pagar <= 0:
                total_a_pagar = None
        except (ValueError, TypeError):
            total_a_pagar = None
    if not total_a_pagar and lance_raw:
        try:
            total_a_pagar = lance_raw + float(valor.get("totalDespesas") or 0)
        except (ValueError, TypeError):
            total_a_pagar = lance_raw

    fotos_raw = item.get("fotosUrls") or produto.get("fotosUrls") or veiculo.get("fotosUrls") or []
    imagens = [
        u for u in (fotos_raw if isinstance(fotos_raw, list) else [])
        if isinstance(u, str) and u.startswith("http")
        and "placeholder" not in u and "logo" not in u
    ]

    loc_obj = item.get("localizacao") or {}
    if isinstance(loc_obj, dict):
        cidade = loc_obj.get("cidade") or loc_obj.get("municipio") or ""
        estado = loc_obj.get("estado") or loc_obj.get("uf") or ""
        localizacao = f"{cidade}/{estado}".strip("/")
    else:
        localizacao = str(loc_obj) if loc_obj else ""

    data_enc = (
        item.get("dataFim") or item.get("dataEncerramento") or
        item.get("dtFim") or item.get("endDate")
    )

    link = f"https://leilo.com.br/leilao/tecnologia/{_slugify(titulo[:60])}/{uuid}"

    return {
        "uuid":              uuid,
        "url":               link,
        "titulo":            titulo,
        "modelo_gpu":        extrair_modelo_gpu(titulo),
        "lance_raw":         lance_raw,
        "lance":             fmt_brl(lance_raw),
        "total_a_pagar_raw": total_a_pagar,
        "total_a_pagar":     fmt_brl(total_a_pagar),
        "imagens":           imagens,
        "localizacao":       localizacao,
        "descricao":         item.get("descricao") or item.get("observacao"),
        "data_encerramento": parse_data(data_enc),
        "preco_mercado":     None,
        "desconto_pct":      None,
        "desconto_label":    "—",
        "margem_bruta":      None,
        "preco_mercado_fmt": "—",
    }


# ─── Fetch paginado ───────────────────────────────────────────────────────────

async def fetch_gpus(
    client: httpx.AsyncClient,
    max_lotes: int = 999,
    debug: bool = False,
) -> list:
    PAGE_SIZE = 30
    offset    = 0
    todos     = []
    total_api = None

    while True:
        payload = _build_payload(offset, PAGE_SIZE)

        if debug:
            print(f"\n  {DIM}── Payload (offset={offset}) ──{RESET}")
            print(f"  {DIM}{json.dumps(payload, ensure_ascii=False, indent=2)}{RESET}")

        try:
            resp = await client.post(API_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  {RED}Erro na API (offset={offset}): {e}{RESET}")
            break

        if debug:
            print(f"  {DIM}status={resp.status_code}  count-header={resp.headers.get('count','?')}{RESET}")

        if total_api is None:
            try:
                total_api = int(resp.headers.get("count", 0)) or None
            except (ValueError, TypeError):
                pass

        items = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            for key in ["lotes", "items", "results", "data", "content", "hits"]:
                if isinstance(data.get(key), list):
                    items = data[key]
                    break
            if not items:
                for v in data.values():
                    if isinstance(v, list) and len(v) > 2:
                        items = v
                        break
            if total_api is None:
                for key in ["total", "totalElements", "count", "totalLotes"]:
                    if isinstance(data.get(key), int):
                        total_api = data[key]
                        break

        if not items:
            if debug:
                print(f"  {RED}Sem items no response:{RESET}")
                print(f"  {DIM}{json.dumps(data, ensure_ascii=False)[:600]}{RESET}")
            break

        antes = len(todos)
        for item in items:
            ex = _extract_item(item)
            if ex:
                todos.append(ex)
            if len(todos) >= max_lotes:
                break

        print(f"  {DIM}offset={offset}  itens={len(items)}  GPUs nesta página={len(todos)-antes}{RESET}")

        if len(todos) >= max_lotes:
            break
        offset += PAGE_SIZE
        if total_api is not None and offset >= total_api:
            break
        if len(items) < PAGE_SIZE:
            break

        await asyncio.sleep(0.3)

    return todos


# ─── Enriquecimento de preços ─────────────────────────────────────────────────

async def enriquecer_precos(gpus: list, debug: bool = False) -> None:
    cache: dict = {}

    for i, gpu in enumerate(gpus, 1):
        modelo = gpu["modelo_gpu"]
        print(f"  {DIM}[{i}/{len(gpus)}] {modelo:<20}{RESET}", end="  ", flush=True)

        if modelo in cache:
            mercado = cache[modelo]
            print(f"{DIM}(cache){RESET}")
        else:
            mercado = await buscar_preco_mercadolivre(modelo, debug=debug)
            cache[modelo] = mercado
            pm = mercado.get("preco_medio")
            if pm:
                print(f"{GREEN}→ {fmt_brl(pm)}  ({mercado['num_resultados']} preços){RESET}")
            else:
                print(f"{YELLOW}sem preço encontrado{RESET}")
            await asyncio.sleep(0.3)

        gpu["preco_mercado"] = mercado

        lance = gpu.get("lance_raw")
        pm    = mercado.get("preco_medio")
        custo = gpu.get("total_a_pagar_raw") or lance

        gpu["desconto_pct"]        = pct_desconto(custo, pm)
        gpu["desconto_label"]      = (
            f"{gpu['desconto_pct']:.1f}% abaixo do mercado"
            if gpu["desconto_pct"] is not None else "—"
        )
        gpu["margem_bruta"]        = round(pm - custo, 2) if (pm and custo) else None
        gpu["custo_aquisicao_raw"] = custo
        gpu["custo_aquisicao"]     = fmt_brl(custo)
        gpu["preco_mercado_fmt"]   = fmt_brl(pm)


# ─── Print ────────────────────────────────────────────────────────────────────

def print_gpu(gpu: dict, i: int, total: int):
    titulo   = (gpu.get("titulo") or "")[:55]
    lance    = gpu.get("lance", "—")
    mercado  = gpu.get("preco_mercado_fmt") or "—"
    desconto = gpu.get("desconto_pct")
    margem   = gpu.get("margem_bruta")
    imgs     = len(gpu.get("imagens") or [])
    modelo   = gpu.get("modelo_gpu", "")
    prefix   = f"  {DIM}[{i:>3}/{total}]{RESET}"
    cor_desc = GREEN if (desconto or 0) >= 20 else (YELLOW if desconto else DIM)
    cor_marg = GREEN if (margem or 0) > 0 else (RED if margem is not None else DIM)

    print(f"{prefix} {YELLOW}{titulo}{RESET}")
    print(
        f"         Lance {GREEN}{lance}{RESET}"
        f"  ·  Mercado {mercado}"
        f"  ·  {cor_desc}{gpu.get('desconto_label','—')}{RESET}"
        f"  ·  Margem {cor_marg}{fmt_brl(margem)}{RESET}"
        f"  ·  {DIM}{modelo}{RESET}"
        f"  ·  🖼 {imgs}\n"
    )


# ─── Upload para Supabase ─────────────────────────────────────────────────────

def upload_to_supabase(gpus: list) -> dict:
    db = SupabaseClient()
    registros, skipped = [], 0

    for gpu in gpus:
        rec = normalize_to_db(gpu)
        if rec:
            registros.append(rec)
        else:
            skipped += 1

    if skipped:
        print(f"  {YELLOW}⚠️  {skipped} GPU(s) ignorada(s) (sem link ou lance){RESET}")
    if not registros:
        print(f"  {RED}Nenhum registro válido para upload.{RESET}")
        return {}

    print(f"\n{BOLD}{'═'*56}{RESET}")
    print(f"{BOLD}  ☁️   UPLOAD → auctions.tecnologia  ({len(registros)} registros){RESET}")
    print(f"{BOLD}{'═'*56}{RESET}\n")

    stats   = db.upsert('tecnologia', registros)
    total_s = stats.get("inserted", 0) + stats.get("updated", 0)
    print(f"\n  ✅  Enviados:         {total_s}  ({stats.get('inserted',0)} novos  +  {stats.get('updated',0)} atualizados)")
    print(f"  🔄  Dupes removidas:  {stats.get('duplicates_removed', 0)}")
    if stats.get('errors'):
        print(f"  ❌  Erros:           {stats.get('errors', 0)}")
    return stats


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Leilo GPU scraper → auctions.tecnologia")
    parser.add_argument("--output",    "-o", default="gpus.json")
    parser.add_argument("--max",       type=int, default=999)
    parser.add_argument("--no-upload", action="store_true", help="Nao sobe pro Supabase")
    parser.add_argument("--no-market", action="store_true", help="Nao busca preco de mercado")
    parser.add_argument("--show",      action="store_true", help="Browser visivel pra cookies")
    parser.add_argument("--debug",     action="store_true", help="Resposta bruta da API")
    args = parser.parse_args()

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  🎮  LEILO GPU SCRAPER → auctions.tecnologia{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  {DIM}upload: {'nao' if args.no_upload else 'sim'}  |  precos: {'nao' if args.no_market else 'sim'}{RESET}\n")

    try:
        cookies = await get_session_cookies(show=args.show)
    except Exception as e:
        print(f"  {RED}Erro ao obter cookies: {e}{RESET}")
        cookies = {}

    try:
        async with httpx.AsyncClient(
            headers=API_HEADERS,
            cookies=cookies,
            follow_redirects=True,
        ) as client:
            print(f"  {CYAN}Consultando API leilo (Equipamentos)...{RESET}")
            gpus = await fetch_gpus(client, max_lotes=args.max, debug=args.debug)
    except Exception as e:
        print(f"  {RED}Erro fatal ao consultar API: {e}{RESET}")
        return

    print(f"\n  {GREEN}✅  {len(gpus)} placas de video encontradas{RESET}\n")

    if not gpus:
        print(f"  {RED}Nenhuma GPU encontrada. Use --debug pra inspecionar a resposta bruta.{RESET}")
        return

    if not args.no_market:
        print(f"\n{BOLD}{'─'*64}{RESET}")
        print(f"{BOLD}  🔍  Buscando precos de mercado (Mercado Livre)...{RESET}")
        print(f"{BOLD}{'─'*64}{RESET}\n")
        try:
            await enriquecer_precos(gpus, debug=args.debug)
        except Exception as e:
            print(f"  {RED}Erro ao buscar precos: {e}{RESET}")

    com_desc  = sorted(
        [g for g in gpus if isinstance(g.get("desconto_pct"), float)],
        key=lambda x: x["desconto_pct"], reverse=True,
    )
    sem_desc  = [g for g in gpus if not isinstance(g.get("desconto_pct"), float)]
    ordenadas = com_desc + sem_desc

    print(f"\n{BOLD}{'─'*64}{RESET}")
    print(f"{BOLD}  📋  RESULTADO{RESET}")
    print(f"{BOLD}{'─'*64}{RESET}\n")
    for i, gpu in enumerate(ordenadas, 1):
        print_gpu(gpu, i, len(ordenadas))

    # Upload Supabase
    stats = {}
    if not args.no_upload:
        try:
            stats = upload_to_supabase(ordenadas)
        except Exception as e:
            print(f"  {RED}Erro no upload Supabase: {e}{RESET}")

    # Salva JSON local
    resultado = {
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "total_gpus":   len(gpus),
        "com_desconto": len(com_desc),
        "gpus":         ordenadas,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  📊  RESUMO{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  Total GPUs:          {len(gpus)}")
    print(f"  Com desconto:        {len(com_desc)}")
    print(f"  Sem desconto/preco:  {len(sem_desc)}")
    if com_desc:
        melhor = com_desc[0]
        print(f"  Melhor oportunidade: {melhor['titulo'][:40]}  →  {melhor['desconto_label']}")
    if not args.no_upload and stats:
        total_s = stats.get("inserted", 0) + stats.get("updated", 0)
        print(f"  Enviados Supabase:   {total_s}  ({stats.get('inserted',0)} novos + {stats.get('updated',0)} atualizados)")
    print(f"  JSON salvo em:       {args.output}\n")


if __name__ == "__main__":
    asyncio.run(main())