#!/usr/bin/env python3
"""
debug_lote.py — Pega o PRIMEIRO lote da listagem do leilo.com.br e printa tudo sem subir nada.

Uso:
    python debug_lote.py                        # pega primeiro lote de carros (padrão)
    python debug_lote.py --categoria motos      # outra categoria
    python debug_lote.py --show                 # abre browser visível
    python debug_lote.py --save-html            # salva o HTML do lote
    python debug_lote.py --url "https://..."    # pula a listagem e vai direto pra URL
"""

import asyncio
import json
import re
import sys
import argparse
from datetime import datetime, timezone, timedelta
from playwright.async_api import async_playwright, Response

# ─── Categorias (idêntico ao leilo.py) ───────────────────────────────────────
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

# ─── Pega UUID links da listagem (idêntico ao leilo.py) ──────────────────────

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


async def get_first_lot_link(page, list_url: str) -> str | None:
    """Abre a listagem e retorna o primeiro link de lote com UUID."""
    print(f"  {DIM}Abrindo listagem: {list_url}{RESET}")
    await page.goto(list_url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(1.5)

    # Scroll pra garantir que os cards carregaram
    for _ in range(3):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(0.4)
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(0.3)

    links = await _get_uuid_links(page)
    return links[0] if links else None

# ─── Cores ────────────────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
MAGENTA = "\033[95m"

# ─── Copia dos helpers do leilo.py ───────────────────────────────────────────

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


_FOTO_KEYS   = {"fotosurls", "fotos", "fotosurl", "photos", "images", "imagens"}
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
)
_SKIP_SUBSTRINGS = ("id", "uuid", "codigo", "numero", "count", "total",
                    "qtd", "quantidade", "sequence", "posicao", "rank",
                    "historico", "history")

# ─── Network capture ──────────────────────────────────────────────────────────

class NetworkCapture:
    def __init__(self):
        self.responses: list[dict] = []
        self._handler = None

    def attach(self, page):
        async def handler(resp: Response):
            if "leilo.com.br" not in resp.url:
                return
            ct = resp.headers.get("content-type", "")
            if "json" not in ct:
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


# ─── DOM JS ───────────────────────────────────────────────────────────────────

DOM_JS = """
() => {
    const out = { pares: {}, rValues: [], imagens: [] };

    out.titulo = (
        document.querySelector('h1')?.innerText ||
        document.querySelector('[class*="titulo-veiculo"]')?.innerText || ''
    ).trim().replace(/\\s+/g, ' ');

    document.querySelectorAll('[class*="label-categoria"]').forEach(label => {
        const col = label.closest('[class*="col-"]');
        if (!col) return;
        const val = col.querySelector('[class*="text-weight-semibold"], p');
        const k = label.innerText?.replace(/\\s+/g, ' ').trim();
        const v = val?.innerText?.replace(/\\s+/g, ' ').trim();
        if (k && v && k !== v && v.length < 200) out.pares[k] = v;
    });

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

    const lanceSelectors = [
        '[class*="lance-atual"]','[class*="lanceAtual"]','[class*="melhor-lance"]',
        '[class*="melhorLance"]','[class*="valor-lance"]','[class*="bid"]',
        '[id*="lance"]','[data-cy*="lance"]'
    ];
    for (const s of lanceSelectors) {
        const el = document.querySelector(s);
        if (el && /R\\$/.test(el.innerText)) { out.lance_dom = el.innerText.trim(); break; }
    }

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

    const imgSeen = new Set();

    // 1ª tentativa: background-image em .q-img__image (padrão do leilo)
    document.querySelectorAll('.q-img__image, [class*="q-img__image"]').forEach(el => {
        const bg = el.style.backgroundImage || window.getComputedStyle(el).backgroundImage || '';
        const m = bg.match(/url\(["']?([^"')]+)["']?\)/);
        if (m) {
            const src = m[1];
            if (src && !imgSeen.has(src) && !src.includes('placeholder')
                     && !src.includes('logo') && src.startsWith('http')) {
                imgSeen.add(src);
                out.imagens.push(src);
            }
        }
    });

    // 2ª tentativa: <img src> nos carrosséis (fallback)
    if (out.imagens.length === 0) {
        const imgSelectors = [
            '[class*="foto-veiculo"] img','[class*="galeria"] img',
            '[class*="carousel"] img','[class*="slider"] img',
            '[class*="vehicle-image"] img','.q-carousel img',
        ];
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
    }

    out.num_lote = (
        document.querySelector('[class*="num-lote"]')?.innerText ||
        document.querySelector('[class*="numero-lote"]')?.innerText || ''
    ).trim();

    out.descricao = (
        document.querySelector('[class*="descricao-lote"]')?.innerText ||
        document.querySelector('[class*="descricao"]')?.innerText || ''
    ).trim().slice(0, 500);

    return out;
}
"""

# ─── Extração de fotos por UUID ───────────────────────────────────────────────

def _extrair_lotes_com_fotos(obj, depth=0) -> list[dict]:
    results = []
    if depth > 10:
        return results
    if isinstance(obj, list):
        for item in obj:
            results.extend(_extrair_lotes_com_fotos(item, depth + 1))
    elif isinstance(obj, dict):
        fotos = None
        for k, v in obj.items():
            kl = k.lower().replace("_", "").replace("-", "")
            if kl in _FOTO_KEYS and isinstance(v, list) and v:
                fotos = [u for u in v if isinstance(u, str) and u.startswith("http")
                         and "placeholder" not in u and "logo" not in u]
                break
        if fotos is not None:
            uuid = None
            for k, v in obj.items():
                if isinstance(v, str):
                    m = _UUID_RE.search(v)
                    if m:
                        uuid = m.group(0).lower()
                        break
            results.append({"uuid": uuid, "fotos": fotos})
        else:
            for v in obj.values():
                results.extend(_extrair_lotes_com_fotos(v, depth + 1))
    return results


def _fotos_para_uuid(responses, lot_uuid):
    for resp in responses:
        lotes = _extrair_lotes_com_fotos(resp["data"])
        for lote in lotes:
            if lote["uuid"] and lote["uuid"].lower() == lot_uuid.lower():
                return lote["fotos"]
    return None


def extract_api_fields(responses, lot_uuid=None) -> dict:
    lance_candidates   = []
    mercado_candidates = []
    extras = {}

    def _key_is_noise(key):
        kl = key.lower().replace("_", "").replace("-", "")
        return any(s in kl for s in _SKIP_SUBSTRINGS)

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
            if any(x in kl for x in ["melhorlance","lancevalor","valorlance","lanceatual",
                                       "bidvalue","currentbid","lastbid","highestbid"]):
                if not _key_is_noise(k):
                    val = parse_brl(v)
                    if val:
                        lance_candidates.append((val, k, v))
            if any(x in kl for x in ["valormercado","valordemercado","precofipe","valorefipe",
                                       "tabelafipe","fipevalor","marketvalue","marketprice"]):
                if not _key_is_noise(k):
                    val = parse_brl(v)
                    if val:
                        mercado_candidates.append((val, k, v))
            if kl in ("km","quilometragem","odometro","kilometragem") and not extras.get("km"):
                try:
                    extras["km"] = int(float(str(v).replace(".","").replace(",",".")))
                except Exception:
                    pass
            if kl in ("cor","cordo","corveiculo") and isinstance(v, str) and 2 < len(v) < 30:
                extras.setdefault("cor", v)
            if ("combustivel" in kl or "fuel" in kl) and isinstance(v, str) and len(v) < 30:
                extras.setdefault("combustivel", v)
            if kl == "retomada" and isinstance(v, str) and 2 < len(v) < 80:
                extras.setdefault("retomada", v)
            if kl in ("anomodelo","anofabricacao","modelyear") and isinstance(v, (int, str)):
                extras.setdefault("ano", str(v))
            if "placa" in kl and isinstance(v, str) and 5 < len(v) < 12:
                extras.setdefault("placa", v)
            if any(x in kl for x in ["dataencerramento","datafim","enddate","dtfim",
                                       "dataauction","encerramento"]):
                if isinstance(v, str) and len(v) > 6:
                    extras.setdefault("data_encerramento_api", v)
            if not lot_uuid:
                if kl in _FOTO_KEYS and isinstance(v, list):
                    urls_validas = [u for u in v if isinstance(u, str) and u.startswith("http")
                                    and "placeholder" not in u and "logo" not in u]
                    if urls_validas:
                        current = extras.get("fotos_api") or []
                        if len(urls_validas) > len(current):
                            extras["fotos_api"] = urls_validas
            walk(v, depth + 1)

    for cap in responses:
        walk(cap["data"])

    result = dict(extras)

    if lot_uuid:
        fotos_por_uuid = _fotos_para_uuid(responses, lot_uuid)
        if fotos_por_uuid:
            result["fotos_api"] = fotos_por_uuid
        elif fotos_por_uuid is not None:
            result.pop("fotos_api", None)

    if lance_candidates:
        lance_candidates.sort(key=lambda x: x[0], reverse=True)
        result["lance"] = lance_candidates[0][2]
        result["_lance_key"] = lance_candidates[0][1]
    if mercado_candidates:
        mercado_candidates.sort(key=lambda x: x[0], reverse=True)
        result["valor_mercado"] = mercado_candidates[0][2]
        result["_mercado_key"] = mercado_candidates[0][1]

    return result


# ─── Printer helpers ─────────────────────────────────────────────────────────

def sec(title: str):
    print(f"\n{BOLD}{CYAN}{'─'*60}{RESET}")
    print(f"{BOLD}{CYAN}  {title}{RESET}")
    print(f"{BOLD}{CYAN}{'─'*60}{RESET}")


def row(label: str, value, color=RESET):
    lpad = f"  {YELLOW}{label:<30}{RESET}"
    print(f"{lpad} {color}{value}{RESET}")


def ok_or_x(val) -> str:
    return f"{GREEN}✓{RESET}" if val else f"{RED}✗{RESET}"


# ─── Main debug ───────────────────────────────────────────────────────────────

async def debug_lote(url: str = None, categoria: str = "carros",
                     show_browser: bool = False, save_html: bool = False):
    print(f"\n{BOLD}{'═'*60}{RESET}")
    print(f"{BOLD}  🔍  DEBUG LOTE — leilo.com.br{RESET}")
    print(f"{BOLD}{'═'*60}{RESET}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not show_browser)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 Chrome/122 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="pt-BR",
        )
        page = await ctx.new_page()

        # Se não veio URL direta, pega o primeiro lote da listagem
        if not url:
            list_url, tipo = CATEGORIAS[categoria]
            print(f"  Categoria: {YELLOW}{categoria.upper()}{RESET}")
            url = await get_first_lot_link(page, list_url)
            if not url:
                print(f"  {RED}❌  Nenhum lote encontrado na listagem. Tente --show (pode ser captcha).{RESET}\n")
                await browser.close()
                return
            print(f"  {GREEN}✓  Primeiro lote: {url}{RESET}")
        else:
            print(f"  URL direta: {DIM}{url}{RESET}")

        print()

        # Captura de rede
        net = NetworkCapture()
        net.attach(page)

        print(f"  {DIM}Carregando lote...{RESET}")
        await page.goto(url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(1.5)

        # Salva HTML se pedido
        if save_html:
            html = await page.content()
            fname = "debug_lote.html"
            with open(fname, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"  {GREEN}HTML salvo em {fname}{RESET}")

        # DOM
        dom = await page.evaluate(DOM_JS)

        # UUID do lote
        lot_uuid_match = _UUID_RE.search(url)
        lot_uuid = lot_uuid_match.group(0).lower() if lot_uuid_match else None

        # Respostas de rede filtradas
        if lot_uuid:
            specific = [r for r in net.responses if lot_uuid in r["url"].lower()]
            responses_to_use = specific if specific else net.responses
        else:
            responses_to_use = net.responses

        api = extract_api_fields(responses_to_use, lot_uuid=lot_uuid)

        net.detach(page)
        await browser.close()

    # ── SEÇÃO 1: REDE ────────────────────────────────────────────────────────
    sec("1. RESPOSTAS DE REDE (JSON)")
    row("UUID do lote", lot_uuid or "não encontrado na URL", MAGENTA)
    row("Total respostas JSON", len(net.responses))
    row("Respostas com UUID", len(responses_to_use))

    print(f"\n  {DIM}URLs capturadas:{RESET}")
    for r in net.responses:
        marker = f" {GREEN}← tem UUID{RESET}" if lot_uuid and lot_uuid in r["url"].lower() else ""
        print(f"    {DIM}{r['url'][:90]}{RESET}{marker}")

    # ── SEÇÃO 2: DOM ─────────────────────────────────────────────────────────
    sec("2. DOM EXTRAÍDO")
    row("Título",          dom.get("titulo") or "—",   YELLOW if dom.get("titulo") else RED)
    row("Lance DOM",       dom.get("lance_dom") or "—", GREEN if dom.get("lance_dom") else DIM)
    row("Num. Lote",       dom.get("num_lote") or "—")
    row("Data enc. DOM",   dom.get("data_encerramento") or "—")
    row("Descrição",       (dom.get("descricao") or "—")[:80])
    row("Imagens DOM",     len(dom.get("imagens") or []))

    print(f"\n  {YELLOW}Pares label→valor:{RESET}")
    pares = dom.get("pares") or {}
    if pares:
        for k, v in pares.items():
            print(f"    {DIM}{k:<28}{RESET} {v}")
    else:
        print(f"    {RED}nenhum par encontrado{RESET}")

    print(f"\n  {YELLOW}Valores R$ no DOM:{RESET}")
    for rv in dom.get("rValues", []):
        print(f"    {GREEN}{rv['text']:<20}{RESET}  cls={DIM}{rv['cls'][:60]}{RESET}")

    # ── SEÇÃO 3: API ─────────────────────────────────────────────────────────
    sec("3. API EXTRAÍDA")
    row("Lance API",        api.get("lance") or "—",        GREEN if api.get("lance") else RED)
    if api.get("_lance_key"):
        row("  └ campo fonte",  api["_lance_key"], DIM)
    row("Valor mercado",    api.get("valor_mercado") or "—", GREEN if api.get("valor_mercado") else DIM)
    if api.get("_mercado_key"):
        row("  └ campo fonte",  api["_mercado_key"], DIM)
    row("KM",               api.get("km") or "—")
    row("Cor",              api.get("cor") or "—")
    row("Combustível",      api.get("combustivel") or "—")
    row("Retomada",         api.get("retomada") or "—")
    row("Ano (API)",        api.get("ano") or "—")
    row("Placa",            api.get("placa") or "—")
    row("Data enc. API",    api.get("data_encerramento_api") or "—")
    row("Fotos API",        len(api.get("fotos_api") or []))

    if api.get("fotos_api"):
        print(f"\n  {YELLOW}URLs das fotos (API):{RESET}")
        for i, url_foto in enumerate(api["fotos_api"][:5], 1):
            print(f"    {DIM}[{i}] {url_foto[:100]}{RESET}")
        if len(api["fotos_api"]) > 5:
            print(f"    {DIM}... +{len(api['fotos_api'])-5} fotos{RESET}")

    # ── SEÇÃO 4: CAMPO FINAL COMPOSTO ────────────────────────────────────────
    sec("4. CAMPO FINAL COMPOSTO (como o scraper montaria)")

    lance_raw = api.get("lance") or dom.get("lance_dom")
    if not lance_raw:
        vals = sorted([parse_brl(r["text"]) for r in dom.get("rValues", [])
                       if parse_brl(r["text"])])
        if len(vals) >= 2:
            lance_raw = vals[0]

    mercado_pares = parse_brl(pares.get("Valor Mercado"))
    mercado_api   = parse_brl(api.get("valor_mercado"))
    lance_f       = parse_brl(lance_raw)

    def mercado_ok(m):
        if not m: return False
        if lance_f and m < lance_f: return False
        if lance_f and m > lance_f * 20: return False
        return True

    if mercado_pares and mercado_ok(mercado_pares):
        mercado_raw = mercado_pares
        mercado_src = "pares DOM"
    elif mercado_api and mercado_ok(mercado_api):
        mercado_raw = mercado_api
        mercado_src = "API"
    else:
        vals = sorted([parse_brl(r["text"]) for r in dom.get("rValues", [])
                       if mercado_ok(parse_brl(r["text"]))], reverse=True)
        mercado_raw = vals[0] if vals else (mercado_pares or mercado_api)
        mercado_src = "rValues DOM"

    desc_pct = None
    if lance_f and mercado_raw:
        desc_pct = round((1 - lance_f / mercado_raw) * 100, 1)

    margem_revenda = None
    if mercado_raw and lance_f:
        margem = round(mercado_raw - lance_f - 15_000, 2)
        margem_revenda = margem if margem >= 10_000 else None

    imagens = api.get("fotos_api") or dom.get("imagens") or []
    data_enc = dom.get("data_encerramento") or api.get("data_encerramento_api")

    row("Lance final",      fmt_brl(lance_raw) if lance_raw else "— ❌ AUSENTE",
        GREEN if lance_raw else RED)
    row("Mercado final",    f"{fmt_brl(mercado_raw)} ({mercado_src})" if mercado_raw else "— (sem FIPE)",
        YELLOW if mercado_raw else DIM)
    row("Desconto %",       f"{desc_pct}%" if desc_pct is not None else "—",
        GREEN if (desc_pct or 0) >= 20 else DIM)
    row("Margem revenda",   f"R$ {margem_revenda:,.0f}" if margem_revenda else "— (< R$10k ou sem FIPE)",
        GREEN if margem_revenda else DIM)
    row("Imagens total",    len(imagens), GREEN if imagens else RED)
    row("  fonte",          "API (fotosUrls)" if api.get("fotos_api") else "DOM",
        DIM)
    row("Data enc.",        data_enc or "— (usará +30 dias)", DIM)

    def pares_or_api_val(pk, ak=None):
        v = pares.get(pk, "—")
        if v and v != "—":
            return v
        return (api.get(ak) if ak else None) or "—"

    row("Ano",          pares_or_api_val("Ano", "ano"))
    row("KM",           pares_or_api_val("Km", "km"))
    row("Cor",          pares_or_api_val("Cor", "cor"))
    row("Combustível",  pares_or_api_val("Combustivel", "combustivel"))
    row("Tipo retomada",pares_or_api_val("Tipo Retomada", "retomada"))
    row("Localização",  pares_or_api_val("Localização"))

    # ── SEÇÃO 5: VALIDAÇÃO ───────────────────────────────────────────────────
    sec("5. CHECKLIST DE CAMPOS OBRIGATÓRIOS")

    checks = [
        ("Título",         bool(dom.get("titulo") and dom.get("titulo") != "—")),
        ("Lance (valor_inicial)", bool(lance_raw)),
        ("Ano",            bool(pares_or_api_val("Ano", "ano") != "—")),
        ("Imagens",        bool(imagens)),
        ("Data encerramento", bool(data_enc)),
        ("Margem ≥ R$10k", bool(margem_revenda)),
    ]

    all_ok = True
    for campo, passou in checks:
        status = f"{GREEN}✓  OK{RESET}" if passou else f"{RED}✗  AUSENTE/FALHOU{RESET}"
        print(f"  {'  ✓' if passou else '  ✗'}  {campo:<35} {status}")
        if not passou:
            all_ok = False

    print(f"\n  {'─'*56}")
    if all_ok:
        print(f"  {BOLD}{GREEN}✅  Lote passaria no filtro — seria enviado ao Supabase{RESET}")
    else:
        print(f"  {BOLD}{RED}❌  Lote SERIA DESCARTADO pelo scraper{RESET}")
    print(f"  {'─'*56}\n")

    # ── SEÇÃO 6: PAYLOAD JSON (o que iria ao banco) ──────────────────────────
    sec("6. PAYLOAD SIMULADO (JSON → auctions.veiculos)")

    def parse_ano_local(ano_str):
        if not ano_str or ano_str == "—": return None, None
        nums = re.findall(r"\d{4}", str(ano_str))
        if len(nums) >= 2:
            return int(nums[0]), int(nums[1])
        if len(nums) == 1:
            return int(nums[0]), int(nums[0])
        return None, None

    def parse_localizacao_local(loc):
        if not loc or loc == "—": return None, None
        m = re.search(r"(.+?)\s*[-/]\s*([A-Z]{2})\s*$", loc.strip())
        if m: return m.group(2).upper(), m.group(1).strip()
        return None, None

    titulo = dom.get("titulo", "—")
    ano_fab, ano_mod = parse_ano_local(pares_or_api_val("Ano", "ano"))
    estado, cidade = parse_localizacao_local(pares_or_api_val("Localização"))

    if not ano_fab:
        m = re.search(r"\b(19[5-9]\d|20[0-3]\d)\b", titulo)
        if m:
            ano_fab = ano_mod = int(m.group(1))

    partes = titulo.strip().split() if titulo != "—" else []
    marca  = partes[0].upper() if partes else None
    modelo = " ".join(partes[1:3]) if len(partes) > 1 else None

    km_val = None
    km_raw = pares_or_api_val("Km", "km")
    if km_raw != "—":
        try:
            km_val = int(re.sub(r"[^\d]", "", str(km_raw)))
        except Exception:
            pass

    data_enc_iso = None
    if data_enc:
        formatos = ["%d/%m/%Y %H:%M","%d/%m/%Y","%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S","%Y-%m-%d"]
        for fmt in formatos:
            try:
                dt = datetime.strptime(data_enc.strip(), fmt)
                data_enc_iso = dt.replace(
                    tzinfo=timezone(timedelta(hours=-3))
                ).isoformat()
                break
            except ValueError:
                continue
    if not data_enc_iso:
        data_enc_iso = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()

    payload = {
        "titulo":                titulo,
        "tipo":                  "carro",  # infere da URL na produção
        "marca":                 marca,
        "modelo":                modelo,
        "estado":                estado,
        "cidade":                cidade,
        "ano_fabricacao":        ano_fab,
        "ano_modelo":            ano_mod,
        "modalidade":            "leilao",
        "valor_inicial":         parse_brl(lance_raw),
        "valor_atual":           parse_brl(lance_raw),
        "data_encerramento":     data_enc_iso,
        "link":                  url,
        "imagem_1":              imagens[0] if len(imagens) > 0 else None,
        "imagem_2":              imagens[1] if len(imagens) > 1 else None,
        "imagem_3":              imagens[2] if len(imagens) > 2 else None,
        "percentual_abaixo_fipe": desc_pct,
        "margem_revenda":        margem_revenda,
        "km":                    km_val,
        "origem":                pares_or_api_val("Tipo Retomada", "retomada"),
        "ativo":                 True,
    }

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Debug do primeiro lote da listagem do leilo.com.br")
    parser.add_argument("--url", help="Pula a listagem e vai direto pra URL do lote")
    parser.add_argument("--categoria", default="carros",
                        choices=list(CATEGORIAS.keys()),
                        help="Categoria da listagem (padrão: carros)")
    parser.add_argument("--show", action="store_true", help="Abre browser visível")
    parser.add_argument("--save-html", action="store_true", help="Salva HTML da página do lote")
    args = parser.parse_args()

    asyncio.run(debug_lote(
        url=args.url,
        categoria=args.categoria,
        show_browser=args.show,
        save_html=args.save_html,
    ))