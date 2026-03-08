#!/usr/bin/env python3
"""
leilo.py — Scraper leilo.com.br + upload direto para auctions.veiculos

Usa a API interna do leilo (busca-elastic) diretamente via httpx.
Playwright só é usado uma vez pra obter os cookies de sessão.

Uso:
    python leilo.py --all                  # scrapa todas as categorias
    python leilo.py --all --no-upload      # debug local sem subir
    python leilo.py --all --show           # browser visível pra pegar cookies
    python leilo.py --max 20               # limita lotes por categoria
    python leilo.py --categoria carros     # só uma categoria
    python leilo.py --debug                # mostra respostas brutas da API
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

# ─── Categorias → (filtro API, tipo DB) ───────────────────────────────────────
CATEGORIAS = {
    "carros":      ("Carros",      "carro"),
    "motos":       ("Motos",       "moto"),
    "pesados":     ("Pesados",     "caminhao"),
    "utilitarios": ("Utilitários", "pickup"),
    "sucatas":     ("Sucatas",     "sucata"),
}

BASE_URL = "https://leilo.com.br/leilao/carros"
API_URL  = "https://api.leilo.com.br/v1/lote/busca-elastic"

API_HEADERS = {
    "accept":          "application/json, text/plain, */*",
    "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "content-type":    "application/json",
    "origin":          "https://leilo.com.br",
    "referer":         "https://leilo.com.br/",
    "user-agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/18.5 Mobile/15E148 Safari/604.1"
    ),
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
}

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


# ─── Helpers de parsing ───────────────────────────────────────────────────────

def parse_ano(ano_str) -> tuple[int | None, int | None]:
    if not ano_str:
        return None, None
    nums = re.findall(r"\d{4}", str(ano_str))
    if len(nums) >= 2:
        fab, mod = int(nums[0]), int(nums[1])
        if 1950 <= fab <= 2030 and fab <= mod <= fab + 2:
            return fab, mod
    if len(nums) == 1:
        ano = int(nums[0])
        if 1950 <= ano <= 2030:
            return ano, ano
    return None, None


def parse_km(v) -> int | None:
    if not v:
        return None
    s = re.sub(r"[^\d]", "", str(v))
    try:
        val = int(s)
        return val if 0 <= val <= 2_000_000 else None
    except Exception:
        return None


def parse_localizacao(loc: str) -> tuple[str | None, str | None]:
    if not loc:
        return None, None
    m = re.search(r"(.+?)\s*[-/]\s*([A-Z]{2})\s*$", loc.strip())
    if m:
        return m.group(2).upper(), m.group(1).strip()
    m2 = re.search(r"\b([A-Z]{2})\b", loc)
    if m2:
        return m2.group(1), None
    return None, None


def parse_data(raw) -> str | None:
    if not raw:
        return None
    s = str(raw).strip()
    # timestamp ms
    try:
        ts = float(s)
        if ts > 1e10:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
    except Exception:
        pass
    # ISO com Z
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


# ─── Normalização → schema auctions.veiculos ─────────────────────────────────

def normalize_to_db(lote: dict, tipo: str) -> dict | None:
    link   = lote.get("url")
    titulo = lote.get("titulo", "")
    if not link or not titulo:
        return None

    lance = lote.get("lance_raw")
    if not lance:
        return None

    ano_fab, ano_mod = parse_ano(lote.get("ano"))
    if not ano_fab:
        m = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", titulo)
        if m:
            ano_fab = ano_mod = int(m.group(1))
        else:
            return None

    data_enc = parse_data(lote.get("data_encerramento"))
    if not data_enc:
        data_enc = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    estado, cidade = parse_localizacao(lote.get("localizacao") or "")
    imagens = lote.get("imagens") or []

    partes = titulo.strip().split()
    marca  = partes[0].upper() if partes else None
    modelo = " ".join(partes[1:3]) if len(partes) > 1 else None

    return {
        "titulo":                 titulo,
        "descricao":              lote.get("descricao") or None,
        "tipo":                   tipo,
        "marca":                  marca,
        "modelo":                 modelo,
        "estado":                 estado,
        "cidade":                 cidade,
        "ano_fabricacao":         ano_fab,
        "ano_modelo":             ano_mod,
        "modalidade":             "leilao",
        "valor_inicial":          lance,
        "valor_atual":            lance,
        "data_encerramento":      data_enc,
        "link":                   link,
        "imagem_1":               imagens[0] if len(imagens) > 0 else None,
        "imagem_2":               imagens[1] if len(imagens) > 1 else None,
        "imagem_3":               imagens[2] if len(imagens) > 2 else None,
        "percentual_abaixo_fipe": lote.get("desconto_pct"),
        "margem_revenda":         lote.get("margem_revenda"),
        "km":                     parse_km(lote.get("km")),
        "origem":                 lote.get("tipo_retomada") or "leilo",
        "ativo":                  True,
    }


# ─── Pegar cookies via Playwright (uma vez só) ───────────────────────────────

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


# ─── Parse de URL ─────────────────────────────────────────────────────────────

def parse_leilo_url(url: str) -> dict:
    """
    Extrai categoria e filtros de qualquer URL de listagem do leilo.
    Ex: https://leilo.com.br/leilao/carros/de.2018/ate.2026?veiculo.anoModelo=2018|2026&veiculo.km=1|147564
    """
    from urllib.parse import urlparse, parse_qs, unquote as _unquote

    CAT_MAP = {
        "carros":      "Carros",
        "motos":       "Motos",
        "pesados":     "Pesados",
        "utilitarios": "Utilitários",
        "sucatas":     "Sucatas",
    }
    LABEL_MAP = {
        "veiculo.anoModelo": "Ano",
        "veiculo.km":        "KM",
        "veiculo.marca":     "Marca",
        "veiculo.modelo":    "Modelo",
    }

    parsed  = urlparse(url)
    parts   = parsed.path.strip("/").split("/")
    cat_path = parts[1] if len(parts) > 1 else "carros"
    cat_api  = CAT_MAP.get(cat_path, "Carros")

    filtros = [
        {"campo": "tipo", "tipo": "exata", "label": "Tipo", "valor": cat_api}
    ]

    for campo, vals in parse_qs(parsed.query).items():
        val   = _unquote(vals[0])
        label = LABEL_MAP.get(campo, campo)
        if "|" in val:
            mn_s, mx_s = val.split("|", 1)
            def _to_num(s):
                try: return int(s)
                except ValueError:
                    try: return float(s)
                    except: return s
            filtros.append({
                "campo": campo, "tipo": "range", "label": label,
                "range": {"min": _to_num(mn_s), "max": _to_num(mx_s)}
            })
        else:
            filtros.append({"campo": campo, "tipo": "exata", "label": label, "valor": val})

    return {
        "categoria_path": cat_path,
        "categoria_api":  cat_api,
        "base_url":       url.split("?")[0],
        "filtros":        filtros,
    }


def _build_payload(categoria_api: str, offset: int, size: int, filtros: list | None = None) -> dict:
    if filtros is None:
        filtros = [
            {"campo": "tipo",              "tipo": "exata", "label": "Tipo", "valor": categoria_api},
            {"campo": "veiculo.anoModelo", "tipo": "range", "label": "Ano",  "range": {"min": 2018, "max": 2026}},
            {"campo": "veiculo.km",        "tipo": "range", "label": "KM",   "range": {"min": 1,    "max": 200000}},
        ]
    return {
        "from": offset,
        "size": size,
        "requisicoesBusca": filtros,
        "listaOrdenacao": [
            {"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}
        ],
    }


# ─── UUID regex ───────────────────────────────────────────────────────────────
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)


# ─── Extração de campos do item da API ───────────────────────────────────────

def _extract_item(item: dict) -> dict | None:
    # UUID — campo "id" raiz é o UUID do lote
    uuid = item.get("id") or ""
    if not uuid:
        m = _UUID_RE.search(str(item))
        uuid = m.group(0).lower() if m else None
    if not uuid:
        return None

    # Sub-dicts
    veiculo = item.get("veiculo") or {}
    valor   = item.get("valor") or {}
    lance_d = valor.get("lance") or {}

    # Título — campo "nome" no raiz
    marca  = (veiculo.get("infocarMarca") or "").strip()
    modelo = (veiculo.get("infocarModelo") or "").strip()
    titulo = (
        item.get("nome") or
        item.get("tituloProduto") or item.get("nomeProduto") or
        f"{marca} {modelo}".strip()
    )
    if not titulo:
        return None

    # Lance atual — item["valor"]["lance"]["valor"]
    lance_raw = None
    v = lance_d.get("valor")
    if v is not None:
        lance_raw = parse_brl(v)
    # fallback: valorProposta
    if not lance_raw:
        v = valor.get("valorProposta")
        if v is not None:
            lance_raw = parse_brl(v)

    # FIPE — item["veiculo"]["valorMercado"] (0 para não-veículos)
    mercado_raw = None
    v = veiculo.get("valorMercado")
    if v:
        parsed = parse_brl(v)
        if parsed and parsed > 0:
            mercado_raw = parsed

    # Fotos — item["fotosUrls"] é array direto
    imagens = []
    fotos_raw = item.get("fotosUrls") or veiculo.get("fotosUrls") or []
    if isinstance(fotos_raw, list):
        imagens = [
            u for u in fotos_raw
            if isinstance(u, str) and u.startswith("http")
            and "placeholder" not in u and "logo" not in u
        ]

    # Ano (2000 = placeholder do leilo para "sem ano")
    ano_mod = veiculo.get("anoModelo") or item.get("anoModelo")
    ano_fab = veiculo.get("anoFabricacao") or item.get("anoFabricacao") or ano_mod
    if ano_mod == 2000 and ano_fab == 2000:
        ano_mod, ano_fab = None, None

    # KM — não existe no response atual; pode vir em carros
    km_val   = item.get("km") or veiculo.get("km") or item.get("quilometragem") or veiculo.get("quilometragem")
    cor      = item.get("cor") or veiculo.get("cor")
    comb     = item.get("combustivel") or veiculo.get("combustivel")
    retomada = veiculo.get("retomada") or item.get("retomada") or item.get("tipoRetomada")
    descricao= item.get("descricao") or item.get("observacao")

    # Localização — item["localizacao"]["cidade/estado"]
    loc_obj = item.get("localizacao") or {}
    if isinstance(loc_obj, dict):
        cidade_v = loc_obj.get("cidade") or loc_obj.get("municipio") or ""
        estado_v = loc_obj.get("estado") or loc_obj.get("uf") or ""
        localizacao = f"{cidade_v}/{estado_v}".strip("/")
    else:
        localizacao = str(loc_obj) if loc_obj else ""

    # Data encerramento
    data_enc = (
        item.get("dataFim") or item.get("dataEncerramento") or
        item.get("dtFim") or item.get("endDate")
    )

    # Link — monta com lelId numérico
    lel_id = item.get("lelId") or item.get("id") or uuid
    link = f"https://leilo.com.br/lote/{lel_id}"

    # Desconto e margem
    desc_pct = pct_desconto(lance_raw, mercado_raw)
    margem_revenda = None
    if mercado_raw and lance_raw:
        margem = round(mercado_raw - lance_raw - 15_000, 2)
        margem_revenda = margem if margem >= 10_000 else None

    ano_str = (
        f"{ano_fab}/{ano_mod}"
        if ano_fab and ano_mod and str(ano_fab) != str(ano_mod)
        else str(ano_mod or ano_fab or "")
    )

    return {
        "uuid":              uuid,
        "url":               link,
        "titulo":            str(titulo).strip(),
        "lance_raw":         lance_raw,
        "lance":             fmt_brl(lance_raw),
        "valor_mercado_raw": mercado_raw,
        "valor_mercado":     fmt_brl(mercado_raw),
        "desconto_pct":      desc_pct,
        "desconto_label":    f"{desc_pct}% abaixo do mercado" if desc_pct else "—",
        "margem_revenda":    margem_revenda,
        "imagens":           imagens,
        "ano":               ano_str,
        "km":                km_val,
        "cor":               cor,
        "combustivel":       comb,
        "tipo_retomada":     retomada,
        "localizacao":       localizacao,
        "descricao":         descricao,
        "data_encerramento": parse_data(data_enc),
    }


# ─── Busca via API ────────────────────────────────────────────────────────────

async def fetch_categoria(
    client: httpx.AsyncClient,
    categoria_api: str,
    max_lotes: int = 999,
    debug: bool = False,
    filtros: list | None = None,
) -> list[dict]:
    PAGE_SIZE = 48
    offset    = 0
    todos     = []
    total_api = None

    while True:
        payload = _build_payload(categoria_api, offset, PAGE_SIZE, filtros)
        try:
            resp = await client.post(API_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  {RED}Erro na API (offset={offset}): {e}{RESET}")
            break

        if debug:
            print(f"  {DIM}offset={offset} status={resp.status_code} "
                  f"count_header={resp.headers.get('count','?')} "
                  f"body_type={type(data).__name__}{RESET}")
            if isinstance(data, list) and data and isinstance(data[0], dict):
                print(f"  {DIM}  sample item keys: {list(data[0].keys())}{RESET}")

        # Paginação — total vem no header "count"
        if total_api is None:
            try:
                total_api = int(resp.headers.get("count", 0)) or None
            except (ValueError, TypeError):
                pass

        # Acha os itens — API retorna lista direta
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
            # total no body como fallback
            if total_api is None:
                for key in ["total", "totalElements", "count", "totalLotes", "totalHits"]:
                    if isinstance(data.get(key), int):
                        total_api = data[key]
                        break

        if not items:
            if debug:
                print(f"  {RED}Nenhum item encontrado no response. Raw:{RESET}")
                print(f"  {DIM}{json.dumps(data, ensure_ascii=False)[:500]}{RESET}")
            break

        for item in items:
            extracted = _extract_item(item)
            if extracted:
                todos.append(extracted)
            if len(todos) >= max_lotes:
                break

        if len(todos) >= max_lotes:
            break

        offset += PAGE_SIZE
        if total_api is not None and offset >= total_api:
            break
        if len(items) < PAGE_SIZE:
            break

        await asyncio.sleep(0.3)

    return todos


# ─── Filtragem ────────────────────────────────────────────────────────────────

def filtrar_lotes(lotes: list[dict]) -> tuple[list[dict], int, int]:
    ok, sem_lance, lixo = [], 0, 0
    for lote in lotes:
        if not lote.get("lance_raw"):
            sem_lance += 1
            continue
        if not lote.get("imagens"):
            lixo += 1
            continue
        if lote.get("margem_revenda") is None:
            lixo += 1
            continue
        ok.append(lote)
    return ok, sem_lance, lixo


def print_lote(lote: dict, i: int, total: int):
    titulo = (lote["titulo"] or "")[:50]
    desc   = lote.get("desconto_pct") or 0
    margem = lote.get("margem_revenda") or 0
    n_imgs = len(lote.get("imagens") or [])
    cor    = GREEN if desc >= 30 else YELLOW
    prefix = f"  {DIM}[{i:>3}/{total}]{RESET}"
    print(f"{prefix} {YELLOW}{titulo}{RESET}")
    print(f"         Lance {GREEN}{lote['lance']}{RESET}  ·  "
          f"Mercado {lote['valor_mercado']}  ·  "
          f"{cor}{lote['desconto_label']}{RESET}"
          f"  ·  Margem {GREEN}R$ {margem:,.0f}{RESET}"
          f"  ·  🖼 {n_imgs} fotos\n")


# ─── Upload para Supabase ─────────────────────────────────────────────────────

def upload_to_supabase(lotes: list[dict], tipo: str) -> dict:
    db = SupabaseClient()
    registros, skipped = [], 0

    for lote in lotes:
        rec = normalize_to_db(lote, tipo)
        if rec:
            registros.append(rec)
        else:
            skipped += 1

    if skipped:
        print(f"  {YELLOW}⚠️  {skipped} lote(s) ignorado(s){RESET}")
    if not registros:
        print(f"  {RED}Nenhum registro válido para upload.{RESET}")
        return {}

    print(f"\n{BOLD}{'═'*56}{RESET}")
    print(f"{BOLD}  ☁️   UPLOAD → auctions.veiculos  ({len(registros)} registros){RESET}")
    print(f"{BOLD}{'═'*56}{RESET}\n")

    stats   = db.upsert_veiculos(registros)
    total_s = stats.get("inserted", 0) + stats.get("updated", 0)
    print(f"\n  ✅  Enviados:         {total_s}  ({stats.get('inserted',0)} novos  +  {stats.get('updated',0)} atualizados)")
    print(f"  🔄  Dupes removidas:  {stats.get('duplicates_removed', 0)}")
    return stats


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Leilo API scraper → auctions.veiculos")
    parser.add_argument("--all",       action="store_true", help="Scrapa todas as categorias")
    parser.add_argument("--categoria", choices=list(CATEGORIAS.keys()), help="Categoria específica")
    parser.add_argument("--output",    "-o", default="lotes.json")
    parser.add_argument("--no-upload", action="store_true", help="Não sobe pro Supabase")
    parser.add_argument("--show",      action="store_true", help="Browser visível pra cookies")
    parser.add_argument("--url",       type=str, default=None, help="URL do leilo para extrair filtros (sobrepõe --categoria)")
    parser.add_argument("--debug",     action="store_true", help="Mostra respostas brutas da API")
    parser.add_argument("--max",       type=int, default=999, help="Máx lotes por categoria")
    args = parser.parse_args()

    if not args.all and not args.categoria and not args.url:
        parser.error("Use --all, --categoria <nome> ou --url <url>")

    targets = (
        list(CATEGORIAS.items())
        if args.all
        else [(args.categoria or "carros", CATEGORIAS.get(args.categoria or "carros", ("Carros", "carro")))]
    )

    # ✅ Extrai filtros da URL se fornecida, senão None (usa filtros padrão da categoria)
    url_filtros = None
    if args.url:
        parsed_url = parse_leilo_url(args.url)
        url_filtros = parsed_url["filtros"]
        # Sobrepõe targets com a categoria detectada na URL
        cat_path = parsed_url["categoria_path"]
        if cat_path in CATEGORIAS:
            targets = [(cat_path, CATEGORIAS[cat_path])]
        print(f"  {CYAN}Filtros extraídos da URL:{RESET}")
        print(f"  {DIM}{json.dumps(url_filtros, ensure_ascii=False, indent=2)}{RESET}\n")

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  🚗  LEILO API SCRAPER → auctions.veiculos{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  {DIM}{len(targets)} categoria(s) | upload: {'não' if args.no_upload else 'sim'}{RESET}\n")

    cookies = await get_session_cookies(show=args.show)

    todos_lotes: list[dict] = []
    stats_total = {"inserted": 0, "updated": 0, "errors": 0, "duplicates_removed": 0}

    async with httpx.AsyncClient(
        headers=API_HEADERS,
        cookies=cookies,
        follow_redirects=True,
    ) as client:

        for cat_slug, (cat_api, tipo) in targets:
            print(f"\n{BOLD}{'─'*64}{RESET}")
            print(f"{BOLD}  📂  {cat_slug.upper()} ({tipo}){RESET}")
            print(f"{BOLD}{'─'*64}{RESET}")

            print(f"  {CYAN}Consultando API...{RESET}")
            lotes_raw = await fetch_categoria(client, cat_api, args.max, args.debug, url_filtros)
            print(f"  {GREEN}✓  {len(lotes_raw)} lotes retornados pela API{RESET}")

            lotes, sem_lance, lixo = filtrar_lotes(lotes_raw)

            if sem_lance:
                print(f"  {DIM}⏭  {sem_lance} sem lance ativo{RESET}")
            if lixo:
                print(f"  {DIM}🗑  {lixo} descartados (sem imagem ou margem < R$10k){RESET}")
            print(f"  {GREEN}✅  {len(lotes)} lotes aprovados{RESET}\n")

            for i, lote in enumerate(lotes, 1):
                print_lote(lote, i, len(lotes))

            if lotes:
                todos_lotes.extend(lotes)
                if not args.no_upload:
                    stats = upload_to_supabase(lotes, tipo)
                    for k in ("inserted", "updated", "errors", "duplicates_removed"):
                        stats_total[k] = stats_total.get(k, 0) + stats.get(k, 0)

    # Salva JSON local
    com = sorted(
        [l for l in todos_lotes if isinstance(l.get("desconto_pct"), float)],
        key=lambda x: x["desconto_pct"], reverse=True,
    )
    sem = [l for l in todos_lotes if not isinstance(l.get("desconto_pct"), float)]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "total_lotes":  len(todos_lotes),
            "com_desconto": len(com),
            "lotes":        com + sem,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  📊  RESUMO FINAL{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  Total coletados:  {len(todos_lotes)}")
    print(f"  Com desconto:     {len(com)}")
    if not args.no_upload:
        total_s = stats_total["inserted"] + stats_total["updated"]
        print(f"  Enviados:         {total_s}  ({stats_total['inserted']} novos + {stats_total['updated']} atualizados)")
        print(f"  Erros:            {stats_total['errors']}")
    print(f"  JSON salvo em:    {args.output}\n")


if __name__ == "__main__":
    asyncio.run(main())