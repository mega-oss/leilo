#!/usr/bin/env python3
"""
leilo_scraper.py - Extrai lotes do leilo.com.br via passive network listening

Uso:
    python leilo_scraper.py "https://leilo.com.br/leilao/goiania-goias/carros/..."
    python leilo_scraper.py "URL" --output resultado.json
    python leilo_scraper.py "URL" --no-details   # só links, sem entrar em cada lote
    python leilo_scraper.py "URL" --debug        # dump das respostas da API (diagnóstico)
    python leilo_scraper.py "URL" --show         # abre janela do browser
"""

import asyncio
import json
import re
import argparse
from playwright.async_api import async_playwright, Response

# ─── Terminal colors ──────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# ─── Helpers monetários ───────────────────────────────────────────────────────

def parse_brl(v) -> float | None:
    """
    Converte qualquer representação de valor pra float.
    'R$ 28.900,00' -> 28900.0  |  28900 -> 28900.0  |  'abc' -> None
    Só aceita valores entre 500 e 5.000.000 (sanidade pra veículos).
    """
    if v is None:
        return None
    s = str(v).replace("R$", "").replace("\xa0", "").replace(" ", "").strip()
    # BR: 28.900,00
    if re.match(r"^\d{1,3}(\.\d{3})+(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    # Virgula como decimal: 28900,00
    elif re.match(r"^\d+(,\d{1,2})$", s):
        s = s.replace(",", ".")
    # Remove qualquer separador de milhar restante
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

# ─── Extração inteligente dos JSONs de API ───────────────────────────────────

# Chaves que NÃO são valores monetários (contadores, IDs, etc.)
_SKIP_SUBSTRINGS = ("id", "uuid", "codigo", "numero", "count", "total",
                    "qtd", "quantidade", "sequence", "posicao", "rank",
                    "historico", "history")

def _key_is_noise(key: str) -> bool:
    kl = key.lower().replace("_", "").replace("-", "")
    return any(s in kl for s in _SKIP_SUBSTRINGS)

def extract_api_fields(responses: list[dict]) -> dict:
    """
    Percorre JSONs capturados extraindo campos com critério:
    - Valores monetários validados por parse_brl (500–5M)
    - Chaves de contagem/ID ignoradas
    - Para lance: prefere chaves explícitas tipo melhorLance/lanceAtual
    - Para mercado: prefere chaves explícitas tipo valorMercado/fipe
    """
    lance_candidates   = []   # (float, chave, valor_original)
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

            # ── Lance atual ──────────────────────────────────────────────────
            is_lance_key = any(x in kl for x in [
                "melhorlance", "lancevalor", "valorlance", "lanceatual",
                "bidvalue", "currentbid", "lastbid", "highestbid",
            ])
            if is_lance_key and not _key_is_noise(k):
                val = parse_brl(v)
                if val:
                    lance_candidates.append((val, k, v))

            # ── Valor de mercado / FIPE ──────────────────────────────────────
            is_mercado_key = any(x in kl for x in [
                "valormercado", "valordemercado", "precofipe", "valorefipe",
                "tabelafipe", "fipevalor", "marketvalue", "marketprice",
            ])
            if is_mercado_key and not _key_is_noise(k):
                val = parse_brl(v)
                if val:
                    mercado_candidates.append((val, k, v))

            # ── KM ──────────────────────────────────────────────────────────
            if kl in ("km", "quilometragem", "odometro", "kilometragem") and not extras.get("km"):
                try:
                    extras["km"] = int(float(str(v).replace(".", "").replace(",", ".")))
                except Exception:
                    pass

            # ── Cor ──────────────────────────────────────────────────────────
            if kl in ("cor", "cordo", "corveiculo") and isinstance(v, str) and 2 < len(v) < 30:
                extras.setdefault("cor", v)

            # ── Combustível ──────────────────────────────────────────────────
            if ("combustivel" in kl or "fuel" in kl) and isinstance(v, str) and len(v) < 30:
                extras.setdefault("combustivel", v)

            # ── Ano ──────────────────────────────────────────────────────────
            if kl in ("anomodelo", "anofabricacao", "modelyear") and isinstance(v, (int, str)):
                extras.setdefault("ano", str(v))

            # ── Placa ────────────────────────────────────────────────────────
            if "placa" in kl and isinstance(v, str) and 5 < len(v) < 12:
                extras.setdefault("placa", v)

            walk(v, depth + 1)

    for cap in responses:
        walk(cap["data"])

    result = dict(extras)

    # Lance: o maior candidato (lance mais alto = mais recente)
    if lance_candidates:
        lance_candidates.sort(key=lambda x: x[0], reverse=True)
        result["lance"] = lance_candidates[0][2]

    # Mercado: o maior candidato (valor de tabela)
    if mercado_candidates:
        mercado_candidates.sort(key=lambda x: x[0], reverse=True)
        result["valor_mercado"] = mercado_candidates[0][2]

    return result

# ─── Extração DOM ────────────────────────────────────────────────────────────

DOM_JS = """
() => {
    const out = { pares: {}, rValues: [] };

    // Título
    out.titulo = (
        document.querySelector('h1')?.innerText ||
        document.querySelector('[class*="titulo-veiculo"]')?.innerText || ''
    ).trim().replace(/\\s+/g, ' ');

    // Pares label → valor (seção categorias)
    document.querySelectorAll('[class*="label-categoria"]').forEach(label => {
        const col = label.closest('[class*="col-"]');
        if (!col) return;
        const val = col.querySelector('[class*="text-weight-semibold"], p');
        const k = label.innerText?.replace(/\\s+/g, ' ').trim();
        const v = val?.innerText?.replace(/\\s+/g, ' ').trim();
        if (k && v && k !== v && v.length < 200) out.pares[k] = v;
    });

    // Todos os valores monetários visíveis na página (folhas do DOM)
    const seen = new Set();
    document.querySelectorAll('*').forEach(el => {
        if (el.children.length > 0) return;
        const t = (el.innerText || '').trim();
        if (!t || seen.has(t)) return;
        seen.add(t);
        if (/R\\$\\s*[\\d,.]+/.test(t)) {
            out.rValues.push({
                text: t,
                cls: typeof el.className === 'string' ? el.className : ''
            });
        }
    });

    // Lance: seletores específicos
    const lanceSelectors = [
        '[class*="lance-atual"]','[class*="lanceAtual"]','[class*="melhor-lance"]',
        '[class*="melhorLance"]','[class*="valor-lance"]','[class*="bid"]',
        '[id*="lance"]','[data-cy*="lance"]'
    ];
    for (const s of lanceSelectors) {
        const el = document.querySelector(s);
        if (el && /R\\$/.test(el.innerText)) {
            out.lance_dom = el.innerText.trim();
            break;
        }
    }

    // Lote número
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

# ─── Listagem ─────────────────────────────────────────────────────────────────


async def _get_uuid_links(page) -> list[str]:
    """Coleta links UUID no DOM sem scroll."""
    return await page.evaluate("""
        () => {
            const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
            const seen = new Set();
            const out = [];
            document.querySelectorAll('a[href]').forEach(a => {
                if (UUID.test(a.href) && !seen.has(a.href)) {
                    seen.add(a.href);
                    out.push(a.href);
                }
            });
            return out;
        }
    """)


async def _max_visible_page(page) -> int:
    """Maior número de página visível nos botões da paginação."""
    return await page.evaluate("""
        () => {
            const btns = document.querySelectorAll('.q-pagination button[aria-label]');
            let max = 1;
            btns.forEach(b => {
                const n = parseInt(b.getAttribute('aria-label'), 10);
                if (!isNaN(n) && n > max) max = n;
            });
            return max;
        }
    """)


async def _next_btn_enabled(page) -> bool:
    """Verifica se o botão > (next) do Quasar está habilitado."""
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
    """Clica no botão numérico pg_num se estiver visível."""
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
    """Clica na seta > para avançar a janela de páginas."""
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
    """Aguarda o Quasar confirmar que a página pg_num está ativa."""
    try:
        await page.wait_for_function(
            "(n) => {"
            "  const b = document.querySelector('.q-pagination button[aria-label]');"
            "  if (!b) return false;"
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


async def _collect_page_links(page, seen: set, all_links: list, pg_num: int) -> int:
    """Scroll leve + coleta links novos da página atual."""
    # Volta ao topo — essencial para o Quasar re-renderizar os cards
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
        new_count = await _collect_page_links(page, seen, all_links, pg_num)
        print(f"  {DIM}  +{new_count} novos links (total: {len(all_links)}){RESET}")

        # Checa se há próxima página
        if not await _next_btn_enabled(page):
            print(f"  {DIM}Última página alcançada ({pg_num}).{RESET}")
            break

        pg_num += 1

        # Tenta clicar direto no botão numérico (pode não estar visível
        # se a janela de paginação ainda não avançou)
        clicked = await _click_page_btn(page, pg_num)
        if not clicked:
            # Botão não visível: avança a janela clicando na seta >
            await _click_next_arrow(page)
            await asyncio.sleep(0.8)
            await page.wait_for_load_state("networkidle")
            # Agora tenta o botão numérico novamente
            clicked = await _click_page_btn(page, pg_num)

        if not clicked:
            # Não foi possível navegar — usa só a seta e coleta o que vier
            print(f"  {DIM}Navegando via seta para pág {pg_num}...{RESET}")

        await _wait_for_page(page, pg_num)

    return all_links

# ─── Detalhe do lote ──────────────────────────────────────────────────────────

async def get_lot_detail(page, url: str, net: NetworkCapture, debug: bool = False) -> dict:
    net.reset()
    net.attach(page)

    await page.goto(url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(1.2)

    dom  = await page.evaluate(DOM_JS)
    api  = extract_api_fields(net.responses)

    if debug:
        print(f"\n  {DIM}── DEBUG ──────────────────────────────────────────{RESET}")
        print(f"  {DIM}API responses: {len(net.responses)}{RESET}")
        for r in net.responses:
            print(f"  {DIM}  {r['url']}")
            print(f"       {json.dumps(r['data'], ensure_ascii=False)[:400]}{RESET}")
        print(f"  {DIM}API extraído: {api}{RESET}")
        print(f"  {DIM}DOM pares:    {dom.get('pares')}{RESET}")
        print(f"  {DIM}rValues:      {dom.get('rValues', [])[:8]}{RESET}")
        print()

    net.detach(page)
    pares = dom.get("pares") or {}

    # ── Lance ─────────────────────────────────────────────────────────────────
    # API é a melhor fonte pra lance (não aparece com label fixo no DOM)
    lance_raw = api.get("lance") or dom.get("lance_dom")

    if not lance_raw:
        # Fallback: menor R$ válido na página com pelo menos 2 valores distintos
        vals = sorted([parse_brl(r["text"]) for r in dom.get("rValues", []) if parse_brl(r["text"])])
        if len(vals) >= 2:
            lance_raw = vals[0]
        # Se só 1 valor, não arrisca (pode ser o mercado)

    # ── Mercado ───────────────────────────────────────────────────────────────
    # DOM pares TEM PRIORIDADE — "Valor Mercado" está explicitamente rotulado na página
    mercado_pares = parse_brl(pares.get("Valor Mercado"))
    mercado_api   = parse_brl(api.get("valor_mercado"))

    # Sanidade: mercado deve ser > lance e < lance * 20 (desconto real máximo ~95%)
    lance_f = parse_brl(lance_raw)
    def mercado_ok(m):
        if not m: return False
        if lance_f and m < lance_f: return False          # mercado < lance → absurdo
        if lance_f and m > lance_f * 20: return False     # > 2000% de desconto → lixo
        return True

    if mercado_pares and mercado_ok(mercado_pares):
        mercado_raw = mercado_pares
    elif mercado_api and mercado_ok(mercado_api):
        mercado_raw = mercado_api
    else:
        # Último fallback: maior R$ razoável na página
        vals = sorted(
            [parse_brl(r["text"]) for r in dom.get("rValues", []) if mercado_ok(parse_brl(r["text"]))],
            reverse=True
        )
        mercado_raw = vals[0] if vals else (mercado_pares or mercado_api)

    # ── Campos do veículo — DOM pares tem prioridade (são os valores exibidos) ──
    def pares_or_api(pares_key, api_key=None):
        v = pares.get(pares_key)
        if v and v != "—":
            # Limpa texto de ícones Material que vaza junto
            v = re.sub(r'\s*(info_outline|info|warning|error)\s*$', '', v, flags=re.I).strip()
            return v or "—"
        return (api.get(api_key) if api_key else None) or "—"

    # Desconto
    desc_pct = pct_desconto(lance_raw, mercado_raw)

    return {
        "url": url,
        "titulo": dom.get("titulo") or "—",
        "lance": fmt_brl(lance_raw) if lance_raw else "—",
        "lance_raw": parse_brl(lance_raw),
        "valor_mercado": fmt_brl(mercado_raw) if mercado_raw else "—",
        "valor_mercado_raw": parse_brl(mercado_raw),
        "desconto_pct": desc_pct,
        "desconto_label": f"{desc_pct}% abaixo do mercado" if desc_pct is not None else "—",
        # Dados do veículo — pares DOM sempre preferido
        "ano":             pares_or_api("Ano", "ano"),
        "km":              pares_or_api("Km", "km"),
        "combustivel":     pares_or_api("Combustivel", "combustivel"),
        "cor":             pares_or_api("Cor", "cor"),
        "chave":           pares_or_api("Possui Chave"),
        "tipo_retomada":   pares_or_api("Tipo Retomada"),
        "localizacao":     pares_or_api("Localização"),
        "prazo_documentacao": pares_or_api("Prazo est. documentação"),
        "descricao":       dom.get("descricao") or "—",
    }

# ─── Main ──────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Scraper leilo.com.br v2")
    parser.add_argument("url", help="URL da listagem")
    parser.add_argument("--output", "-o", default="lotes.json")
    parser.add_argument("--no-details", action="store_true", help="Só links")
    parser.add_argument("--show", action="store_true", help="Janela do browser visível")
    parser.add_argument("--debug", action="store_true", help="Dump das respostas de API")
    parser.add_argument("--max", type=int, default=999, help="Máx de lotes")
    args = parser.parse_args()

    print(f"\n{BOLD}{'═'*64}{RESET}")
    print(f"{BOLD}  🚗  LEILO SCRAPER  v2{RESET}")
    print(f"{BOLD}{'═'*64}{RESET}")
    print(f"  {DIM}{args.url}{RESET}\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.show)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36",
            viewport={"width": 1280, "height": 900},
            locale="pt-BR",
        )
        net = NetworkCapture()

        # ── Listagem ──────────────────────────────────────────────────────────
        page_list = await ctx.new_page()
        lot_links = await get_lot_links(page_list, args.url)
        lot_links = list(dict.fromkeys(lot_links))[:args.max]

        print(f"{GREEN}  ✅ {len(lot_links)} lotes encontrados{RESET}\n")

        if not lot_links:
            print(f"{RED}  ⚠️  Nenhum lote. Tente --show (pode ter captcha).{RESET}\n")
            await browser.close()
            return

        if args.no_details:
            out = {"listagem_url": args.url, "total": len(lot_links), "links": lot_links}
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(out, f, ensure_ascii=False, indent=2)
            print(f"{GREEN}  💾 Salvo: {args.output}{RESET}\n")
            await browser.close()
            return

        # ── Detalhe de cada lote ──────────────────────────────────────────────
        page_det = await ctx.new_page()
        lotes = []
        skipped = 0

        for i, link in enumerate(lot_links, 1):
            print(f"  {DIM}[{i:>3}/{len(lot_links)}]{RESET} ...{link[-50:]}")
            try:
                lote = await get_lot_detail(page_det, link, net, debug=args.debug)

                # Sem lance ativo (encerrado/offline) — ignora
                if not lote.get("lance_raw"):
                    skipped += 1
                    print(f"         {DIM}⏭  sem lance ativo — ignorado{RESET}\n")
                    continue

                lotes.append(lote)
                titulo = (lote["titulo"] or "")[:50]
                desc_pct = lote.get("desconto_pct")
                cor = GREEN if (desc_pct or 0) >= 30 else YELLOW
                print(f"         {YELLOW}{titulo}{RESET}")
                print(f"         Lance {GREEN}{lote['lance']}{RESET}  ·  "
                      f"Mercado {lote['valor_mercado']}  ·  "
                      f"{cor}{lote['desconto_label']}{RESET}\n")

            except Exception as e:
                print(f"         {RED}ERRO: {e}{RESET}\n")
                lotes.append({"url": link, "erro": str(e)})

        if skipped:
            print(f"  {DIM}⏭  {skipped} lote(s) ignorado(s) — sem lance ativo{RESET}\n")

        await browser.close()

        # ── Ordenar e salvar ──────────────────────────────────────────────────
        com = sorted([l for l in lotes if isinstance(l.get("desconto_pct"), float)],
                     key=lambda x: x["desconto_pct"], reverse=True)
        sem = [l for l in lotes if not isinstance(l.get("desconto_pct"), float)]

        output = {
            "listagem_url": args.url,
            "total_lotes": len(lotes),
            "com_desconto_calculado": len(com),
            "melhor_desconto": com[0] if com else None,
            "lotes": com + sem,
        }

        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        # ── Ranking final ─────────────────────────────────────────────────────
        rank_lines = []
        rank_lines.append(f"{'#':<3}  {'%':>6}  {'Lance':>14}  {'Mercado':>14}  {'Veículo':<42}  URL")
        rank_lines.append("─" * 130)

        for j, l in enumerate(com, 1):
            pct_s  = f"{l['desconto_pct']:.1f}%"
            lance  = l.get('lance', '—')
            merc   = l.get('valor_mercado', '—')
            titulo = (l.get('titulo') or '—')[:42]
            url    = l['url']
            rank_lines.append(f"{j:<3}  {pct_s:>6}  {lance:>14}  {merc:>14}  {titulo:<42}  {url}")

        # Erros no final
        if sem:
            rank_lines.append("")
            rank_lines.append(f"── Sem desconto calculado ({len(sem)}) ──")
            for l in sem:
                rank_lines.append(f"     {l.get('titulo','—')}  {l['url']}")

        # Printa no terminal
        print(f"\n{BOLD}{'═'*64}{RESET}")
        print(f"{BOLD}  📊  RANKING — {len(com)} lotes, maior → menor desconto{RESET}")
        print(f"{BOLD}{'═'*64}{RESET}\n")
        for line in rank_lines:
            # Colore a % dependendo do valor
            if line and line[0].isdigit():
                pct_val = float(line.split('%')[0].split()[-1]) if '%' in line else 0
                cor = GREEN if pct_val >= 50 else (YELLOW if pct_val >= 35 else RESET)
                print(f"  {cor}{line}{RESET}")
            else:
                print(f"  {DIM}{line}{RESET}")

        # Salva rank.txt
        rank_file = args.output.replace(".json", "_rank.txt")
        with open(rank_file, "w", encoding="utf-8") as f:
            f.write(f"LEILO RANK — {args.url}\n\n")
            f.write("\n".join(rank_lines))
            f.write("\n")

        print(f"\n  {GREEN}💾 JSON:  {args.output}{RESET}")
        print(f"  {GREEN}📋 Rank:  {rank_file}{RESET}\n")

if __name__ == "__main__":
    asyncio.run(main())