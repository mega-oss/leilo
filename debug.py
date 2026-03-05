#!/usr/bin/env python3
"""
debug_imagens.py — Pega UM único lote do leilo e despeja JSON completo.
Foco: diagnóstico de imagens.

Uso:
    python debug_imagens.py "https://leilo.com.br/leilao/lote/<uuid>"
    python debug_imagens.py --auto   # pega o 1º lote da categoria carros automaticamente
"""

import asyncio
import json
import sys
import argparse
from playwright.async_api import async_playwright, Response

# ─── Configuração ─────────────────────────────────────────────────────────────

DEFAULT_CATEGORY_URL = (
    "https://leilo.com.br/leilao/carros/de.2018/ate.2026"
    "?veiculo.anoModelo=2018%7C2026&veiculo.km=226%7C152908"
)


# ─── DOM JS — versão ampliada para debug de imagens ──────────────────────────

DOM_JS_DEBUG = """
() => {
    const out = {
        titulo: '',
        pares: {},
        rValues: [],
        imagens: [],
        imagens_debug: [],   // todas as imgs encontradas, sem filtro
        lance_dom: null,
        data_encerramento: null,
        num_lote: '',
        descricao: '',
        html_snapshot: '',   // primeiros 2000 chars do body
    };

    // ── Título ────────────────────────────────────────────────────────────────
    out.titulo = (
        document.querySelector('h1')?.innerText ||
        document.querySelector('[class*="titulo-veiculo"]')?.innerText || ''
    ).trim().replace(/\\s+/g, ' ');

    // ── Pares label → valor ───────────────────────────────────────────────────
    document.querySelectorAll('[class*="label-categoria"]').forEach(label => {
        const col = label.closest('[class*="col-"]');
        if (!col) return;
        const val = col.querySelector('[class*="text-weight-semibold"], p');
        const k = label.innerText?.replace(/\\s+/g, ' ').trim();
        const v = val?.innerText?.replace(/\\s+/g, ' ').trim();
        if (k && v && k !== v && v.length < 200) out.pares[k] = v;
    });

    // ── Valores monetários visíveis ───────────────────────────────────────────
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

    // ── Lance DOM ─────────────────────────────────────────────────────────────
    const lanceSelectors = [
        '[class*="lance-atual"]','[class*="lanceAtual"]','[class*="melhor-lance"]',
        '[class*="melhorLance"]','[class*="valor-lance"]','[class*="bid"]',
        '[id*="lance"]','[data-cy*="lance"]'
    ];
    for (const s of lanceSelectors) {
        const el = document.querySelector(s);
        if (el && /R\\$/.test(el.innerText)) { out.lance_dom = el.innerText.trim(); break; }
    }

    // ── Data encerramento ─────────────────────────────────────────────────────
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

    // ── IMAGENS — estratégia em camadas ───────────────────────────────────────

    // 1. Seletores específicos do leilo (galeria, carousel, slider)
    const imgSelectors = [
        '[class*="foto-veiculo"] img',
        '[class*="galeria"] img',
        '[class*="carousel"] img',
        '[class*="slider"] img',
        '[class*="vehicle-image"] img',
        '.q-carousel img',
        '[class*="swiper"] img',
        '[class*="lightbox"] img',
        '[class*="thumbnail"] img',
        '[class*="photo"] img',
        '[class*="imagem"] img',
        'picture img',
    ];

    const imgSeen = new Set();

    // Coleta todas as imgs encontradas via seletores
    for (const s of imgSelectors) {
        document.querySelectorAll(s).forEach(img => {
            const src = img.src || img.dataset.src || img.dataset.lazySrc || '';
            const srcset = img.srcset || '';
            const bestSrc = src || (srcset.split(' ')[0]);
            if (bestSrc && !imgSeen.has(bestSrc)) {
                imgSeen.add(bestSrc);
                if (!bestSrc.includes('placeholder') && !bestSrc.includes('logo')
                    && bestSrc.startsWith('http')) {
                    out.imagens.push(bestSrc);
                }
            }
        });
    }

    // 2. Fallback: TODAS as imagens da página (para debug)
    const allImgSeen = new Set();
    document.querySelectorAll('img').forEach(img => {
        const src = img.src || img.dataset.src || img.dataset.lazySrc || '';
        if (src && !allImgSeen.has(src) && src.startsWith('http')) {
            allImgSeen.add(src);
            out.imagens_debug.push({
                src,
                alt: img.alt || '',
                cls: typeof img.className === 'string' ? img.className : '',
                width: img.naturalWidth,
                height: img.naturalHeight,
                parent_cls: typeof img.parentElement?.className === 'string'
                            ? img.parentElement.className : '',
            });
        }
    });

    // 3. Procura background-image via computed style
    const bgImgs = [];
    document.querySelectorAll('[class*="galeria"], [class*="carousel"], [class*="slider"], [class*="foto"]').forEach(el => {
        const bg = window.getComputedStyle(el).backgroundImage;
        if (bg && bg !== 'none' && bg.includes('url')) {
            const m = bg.match(/url\\(["']?([^"')]+)["']?\\)/);
            if (m && m[1].startsWith('http') && !imgSeen.has(m[1])) {
                imgSeen.add(m[1]);
                out.imagens.push(m[1]);
                bgImgs.push(m[1]);
            }
        }
    });
    out.bg_imagens = bgImgs;

    // ── Lote e Descrição ──────────────────────────────────────────────────────
    out.num_lote = (
        document.querySelector('[class*="num-lote"]')?.innerText ||
        document.querySelector('[class*="numero-lote"]')?.innerText || ''
    ).trim();

    out.descricao = (
        document.querySelector('[class*="descricao-lote"]')?.innerText ||
        document.querySelector('[class*="descricao"]')?.innerText || ''
    ).trim().slice(0, 500);

    // ── Snapshot do HTML (para análise estrutural) ────────────────────────────
    out.html_snapshot = document.body?.innerHTML?.slice(0, 3000) || '';

    return out;
}
"""


# ─── Network capture ──────────────────────────────────────────────────────────

class NetworkCapture:
    def __init__(self):
        self.responses = []
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


# ─── Pega 1º link de lote da categoria ───────────────────────────────────────

async def get_first_lot_link(page, category_url: str) -> str | None:
    import re
    UUID = re.compile(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.I
    )
    print(f"  → Abrindo listagem: {category_url}")
    await page.goto(category_url, wait_until="networkidle", timeout=60000)
    await asyncio.sleep(2)

    # Scroll para carregar lazy
    for _ in range(3):
        await page.evaluate("window.scrollBy(0, window.innerHeight)")
        await asyncio.sleep(0.5)

    links = await page.evaluate("""
        () => {
            const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
            return Array.from(document.querySelectorAll('a[href]'))
                .map(a => a.href)
                .filter(h => UUID.test(h));
        }
    """)

    if links:
        print(f"  ✅ Encontrou {len(links)} lotes. Usando o 1º: {links[0]}")
        return links[0]
    print("  ❌ Nenhum lote encontrado na listagem.")
    return None


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Debug de imagens — 1 lote do leilo")
    parser.add_argument("url", nargs="?", help="URL direta do lote")
    parser.add_argument("--auto", action="store_true",
                        help="Busca automaticamente o 1º lote da categoria carros")
    parser.add_argument("--show", action="store_true", help="Abre janela do browser")
    parser.add_argument("--out", default="debug_lote.json", help="Arquivo de saída")
    args = parser.parse_args()

    if not args.url and not args.auto:
        parser.error("Informe a URL do lote ou use --auto")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=not args.show)
        ctx = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/122 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="pt-BR",
        )
        page = await ctx.new_page()

        lot_url = args.url
        if args.auto:
            lot_url = await get_first_lot_link(page, DEFAULT_CATEGORY_URL)
            if not lot_url:
                await browser.close()
                return

        print(f"\n  → Abrindo lote: {lot_url}")
        net = NetworkCapture()
        net.attach(page)

        await page.goto(lot_url, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(2)

        # Scroll para ativar lazy load das imagens
        print("  → Scroll para lazy-load...")
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 400)")
            await asyncio.sleep(0.4)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.5)

        # Extrai DOM
        print("  → Extraindo DOM...")
        dom = await page.evaluate(DOM_JS_DEBUG)

        # Extrai API
        api_responses = [{"url": r["url"], "data": r["data"]} for r in net.responses]
        net.detach(page)

        await browser.close()

    # ── Monta resultado final ─────────────────────────────────────────────────
    result = {
        "lote_url": lot_url,
        "titulo": dom.get("titulo"),
        "lance_dom": dom.get("lance_dom"),
        "data_encerramento": dom.get("data_encerramento"),
        "num_lote": dom.get("num_lote"),
        "descricao": dom.get("descricao"),
        "pares": dom.get("pares"),
        "rValues": dom.get("rValues"),

        # ── IMAGENS ──────────────────────────────────────────────────────────
        "imagens_encontradas_pelo_seletor": dom.get("imagens"),   # lista final filtrada
        "imagens_background": dom.get("bg_imagens"),              # via CSS background-image
        "imagens_debug_all": dom.get("imagens_debug"),            # TODAS as <img> da página

        # ── API ───────────────────────────────────────────────────────────────
        "api_responses_count": len(api_responses),
        "api_responses": api_responses,                           # respostas JSON da rede
    }

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    # ── Resumo no terminal ────────────────────────────────────────────────────
    print("\n" + "═" * 60)
    print(f"  RESULTADO — {dom.get('titulo', '?')}")
    print("═" * 60)
    print(f"  Lance DOM:   {dom.get('lance_dom', '—')}")
    print(f"  Data enc.:   {dom.get('data_encerramento', '—')}")
    print(f"  Pares DOM:   {list(dom.get('pares', {}).keys())}")
    print()
    print(f"  🖼  imagens (seletor): {len(dom.get('imagens', []))}")
    for img in dom.get("imagens", []):
        print(f"       {img}")
    print()
    print(f"  🖼  background-images: {len(dom.get('bg_imagens', []))}")
    for bg in dom.get("bg_imagens", []):
        print(f"       {bg}")
    print()
    print(f"  🖼  TODAS as <img> da página: {len(dom.get('imagens_debug', []))}")
    for img in dom.get("imagens_debug", []):
        print(f"       [{img['width']}x{img['height']}] cls={img['cls'][:40]}  → {img['src'][:80]}")
    print()
    print(f"  📡 API responses: {len(api_responses)}")
    for r in api_responses[:5]:
        print(f"       {r['url']}")
    print()
    print(f"  💾 JSON salvo em: {args.out}")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())