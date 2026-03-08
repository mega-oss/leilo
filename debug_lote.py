#!/usr/bin/env python3
"""
leilo_debug_imagens.py — Inspeciona como as imagens vêm na API do leilo

Busca alguns lotes e imprime tudo relacionado a imagens:
  - Todos os campos com "foto", "img", "image", "url" no nome
  - Estrutura completa do item raw (JSON salvo em debug_raw.json)

Uso:
    python leilo_debug_imagens.py                    # 5 lotes de carros
    python leilo_debug_imagens.py --categoria motos  # outra categoria
    python leilo_debug_imagens.py --max 10           # mais lotes
    python leilo_debug_imagens.py --show             # browser visível
"""

import asyncio
import httpx
import json
import re
import sys
import argparse
from pathlib import Path
from playwright.async_api import async_playwright

# ─── Cores ────────────────────────────────────────────────────────────────────
CYAN   = "\033[96m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

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
}

CATEGORIAS = {
    "carros":      "Carros",
    "motos":       "Motos",
    "pesados":     "Pesados",
    "utilitarios": "Utilitários",
    "sucatas":     "Sucatas",
}


# ─── Cookies via Playwright ───────────────────────────────────────────────────

async def get_session_cookies(show: bool = False) -> dict:
    print(f"  {DIM}Obtendo cookies via browser...{RESET}")
    cookies = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not show,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(
            user_agent=API_HEADERS["user-agent"],
            viewport={"width": 390, "height": 844},
            locale="pt-BR",
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
    print(f"  {GREEN}✓  {len(cookies)} cookies obtidos{RESET}\n")
    return cookies


# ─── Busca raw (sem filtrar nada) ─────────────────────────────────────────────

async def fetch_raw(client: httpx.AsyncClient, categoria_api: str, n: int) -> list[dict]:
    payload = {
        "from": 0,
        "size": n,
        "requisicoesBusca": [
            {"campo": "tipo", "tipo": "exata", "label": "Tipo", "valor": categoria_api}
        ],
        "listaOrdenacao": [
            {"campo": "dataFim", "tipoCampo": "long", "tipoOrdenacao": "asc"}
        ],
    }
    resp = await client.post(API_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return data
    for key in ["lotes", "items", "results", "data", "content", "hits"]:
        if isinstance(data.get(key), list):
            return data[key]
    return []


# ─── Encontra recursivamente todos os campos de imagem num dict ───────────────

IMAGE_KEYWORDS = re.compile(r"foto|img|imag|url|photo|picture|thumb|banner|midia|media", re.I)

def find_image_fields(obj, path="", results=None):
    if results is None:
        results = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            full_path = f"{path}.{k}" if path else k
            if IMAGE_KEYWORDS.search(k):
                results[full_path] = v
            find_image_fields(v, full_path, results)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            find_image_fields(v, f"{path}[{i}]", results)
    return results


# ─── Impressão de debug ───────────────────────────────────────────────────────

def print_item_debug(item: dict, idx: int):
    titulo = (
        item.get("nome") or item.get("tituloProduto") or
        item.get("nomeProduto") or f"item_{idx}"
    )
    lel_id = item.get("lelId") or item.get("id") or "?"

    print(f"\n{'─'*70}")
    print(f"{BOLD}{YELLOW}[{idx}] {titulo}{RESET}")
    print(f"{DIM}    lelId: {lel_id}{RESET}")
    print(f"{'─'*70}")

    # ── Campos de imagem encontrados ─────────────────────────────────────────
    img_fields = find_image_fields(item)
    if img_fields:
        print(f"\n  {CYAN}{BOLD}Campos de imagem encontrados:{RESET}")
        for path, val in img_fields.items():
            if isinstance(val, list):
                print(f"\n  {GREEN}{path}{RESET}  →  lista com {len(val)} itens")
                for i, v in enumerate(val[:5]):  # mostra até 5
                    print(f"      [{i}]  {v}")
                if len(val) > 5:
                    print(f"      {DIM}... (+{len(val)-5} omitidos){RESET}")
            elif isinstance(val, str) and val:
                print(f"  {GREEN}{path}{RESET}  →  {val}")
            elif val is not None:
                print(f"  {GREEN}{path}{RESET}  →  {repr(val)}")
    else:
        print(f"\n  {RED}⚠  Nenhum campo de imagem encontrado neste item{RESET}")

    # ── Campos do sub-dict "veiculo" (resumo) ────────────────────────────────
    veiculo = item.get("veiculo") or {}
    if veiculo:
        veiculo_keys = list(veiculo.keys())
        print(f"\n  {DIM}Keys em item.veiculo: {veiculo_keys}{RESET}")

    # ── Todas as keys do item raiz ────────────────────────────────────────────
    print(f"\n  {DIM}Keys raiz: {list(item.keys())}{RESET}")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Debug de imagens da API do leilo")
    parser.add_argument("--categoria", choices=list(CATEGORIAS.keys()), default="carros")
    parser.add_argument("--max",  type=int, default=5, help="Qtd de lotes a inspecionar")
    parser.add_argument("--show", action="store_true", help="Browser visível")
    parser.add_argument("--output", default="debug_raw.json", help="Arquivo JSON com os raws")
    args = parser.parse_args()

    cat_api = CATEGORIAS[args.categoria]

    print(f"\n{BOLD}{'═'*70}{RESET}")
    print(f"{BOLD}  🔍  LEILO DEBUG — IMAGENS  ({args.categoria.upper()}  ·  {args.max} lotes){RESET}")
    print(f"{BOLD}{'═'*70}{RESET}\n")

    cookies = await get_session_cookies(show=args.show)

    async with httpx.AsyncClient(
        headers=API_HEADERS,
        cookies=cookies,
        follow_redirects=True,
    ) as client:
        items = await fetch_raw(client, cat_api, args.max)

    print(f"  {GREEN}✓  {len(items)} itens retornados pela API{RESET}")

    if not items:
        print(f"  {RED}Nenhum item retornado. Tente --show para ver o browser.{RESET}")
        return

    # ── Salva JSON raw completo ───────────────────────────────────────────────
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"  {DIM}JSON raw salvo em: {args.output}{RESET}")

    # ── Inspeciona cada item ──────────────────────────────────────────────────
    for i, item in enumerate(items, 1):
        print_item_debug(item, i)

    # ── Resumo geral: quais campos de imagem existem em TODOS os itens ────────
    print(f"\n\n{'═'*70}")
    print(f"{BOLD}  📊  RESUMO — campos de imagem por item{RESET}")
    print(f"{'═'*70}")

    campo_contagem: dict[str, int] = {}
    campo_exemplos: dict[str, list] = {}

    for item in items:
        fields = find_image_fields(item)
        for path, val in fields.items():
            # Normaliza path removendo índices de array para agrupar
            norm = re.sub(r"\[\d+\]", "[]", path)
            campo_contagem[norm] = campo_contagem.get(norm, 0) + 1
            if norm not in campo_exemplos and val:
                exemplo = val[0] if isinstance(val, list) and val else val
                if isinstance(exemplo, str):
                    campo_exemplos[norm] = exemplo

    for campo, count in sorted(campo_contagem.items(), key=lambda x: -x[1]):
        bar = "█" * count + "░" * (len(items) - count)
        exemplo = campo_exemplos.get(campo, "")
        if isinstance(exemplo, str) and len(exemplo) > 60:
            exemplo = exemplo[:57] + "..."
        print(f"  {GREEN}{campo:<45}{RESET}  {bar}  {count}/{len(items)}")
        if exemplo:
            print(f"    {DIM}ex: {exemplo}{RESET}")

    print(f"\n  {DIM}Veja o JSON completo em: {args.output}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())