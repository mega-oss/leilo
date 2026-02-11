#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LEILO.COM.BR - SCRAPER COMPLETO
✅ Coleta 7 categorias via API REST
✅ Normalização de dados para Supabase
✅ Deduplicação automática
✅ Sistema de heartbeat
"""

import sys
import json
import time
import requests
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Adiciona path parent para importar supabase_client
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from supabase_client import SupabaseClient
except:
    SupabaseClient = None


class LeiloScraper:
    """Scraper Leilo.com.br - API REST"""
    
    def __init__(self, debug=False):
        self.source = 'leilo'
        self.api_url = "https://api.leilo.com.br/v1/lote/busca-elastic"
        self.debug = debug
        
        # 7 Categorias principais
        self.categories = [
            'Carros',
            'Motos',
            'Caminhões',
            'Ônibus',
            'Máquinas',
            'Imóveis',
            'Equipamentos',
        ]
        
        self.stats = {
            'total_scraped': 0,
            'duplicates': 0,
            'with_bids': 0,
            'errors': 0,
            'by_category': {},
        }
        
        # Headers para API
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/json",
            "origin": "https://leilo.com.br",
            "referer": "https://leilo.com.br/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Config de paginação
        self.items_per_page = 30
        self.max_pages_per_category = 100  # Limite de segurança
        self.delay_between_requests = 1  # Respeitar o servidor
    
    def scrape(self) -> List[Dict]:
        """Coleta dados de todas as categorias"""
        print("\n" + "="*70)
        print("🔵 LEILO.COM.BR - SCRAPER COMPLETO")
        print("="*70)
        
        all_lots = []
        seen_lot_ids = set()  # Deduplicação
        
        for category in self.categories:
            category_start = time.time()
            
            print(f"\n📦 {category.upper()}")
            print(f"  ⏳ Coletando lotes...")
            
            category_lots = []
            page = 0
            total_elements = None
            consecutive_empty = 0
            max_consecutive_empty = 3
            
            while page < self.max_pages_per_category:
                from_index = page * self.items_per_page
                
                if self.debug:
                    print(f"    📄 Página {page + 1} (from={from_index})...")
                
                # Faz request para API
                response_data = self._make_request(
                    category=category,
                    from_index=from_index,
                    size=self.items_per_page
                )
                
                if response_data is None:
                    print(f"    ⚠️ Erro na requisição - tentando novamente...")
                    time.sleep(5)
                    continue
                
                # Extrai lotes da resposta
                lotes = []
                
                if isinstance(response_data, dict):
                    # Resposta padrão com 'content'
                    if "content" in response_data:
                        lotes = response_data.get("content", [])
                        total_elements = response_data.get("totalElements", 0)
                    else:
                        if self.debug:
                            print(f"    ⚠️ Resposta sem 'content': {list(response_data.keys())}")
                        break
                elif isinstance(response_data, list):
                    # Resposta direta como lista
                    lotes = response_data
                
                if not lotes:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        if self.debug:
                            print(f"    ✅ {consecutive_empty} páginas vazias consecutivas - fim da categoria")
                        break
                    time.sleep(self.delay_between_requests)
                    page += 1
                    continue
                
                # Reset contador de páginas vazias
                consecutive_empty = 0
                
                # Processa lotes da página
                new_lots = 0
                for lote in lotes:
                    lot_id = lote.get('id') or lote.get('lelId')
                    if lot_id and lot_id not in seen_lot_ids:
                        seen_lot_ids.add(lot_id)
                        category_lots.append(lote)
                        new_lots += 1
                
                if self.debug:
                    print(f"    📥 +{new_lots} lotes únicos | Total da categoria: {len(category_lots)}")
                
                # Verifica se chegou ao fim
                if total_elements and len(category_lots) >= total_elements:
                    if self.debug:
                        print(f"    ✅ Todos os {total_elements} lotes da categoria coletados")
                    break
                
                # Se retornou menos itens que o esperado, pode ser última página
                if len(lotes) < self.items_per_page:
                    if self.debug:
                        print(f"    ✅ Última página (retornou {len(lotes)} < {self.items_per_page})")
                    break
                
                # Delay entre requests
                time.sleep(self.delay_between_requests)
                page += 1
            
            # Resumo da categoria
            category_time = time.time() - category_start
            self.stats['by_category'][category] = len(category_lots)
            all_lots.extend(category_lots)
            
            print(f"  ✅ {category}: {len(category_lots)} lotes em {category_time:.1f}s")
        
        self.stats['total_scraped'] = len(all_lots)
        
        print(f"\n📊 COLETA FINALIZADA")
        print(f"  • Total coletado: {len(all_lots)} lotes")
        print(f"  • Categorias processadas: {len(self.categories)}")
        
        return all_lots
    
    def _make_request(self, category: str, from_index: int = 0, size: int = 30) -> Optional[Dict]:
        """
        Faz requisição POST para a API da Leilo
        
        Args:
            category: Categoria (Carros, Motos, etc)
            from_index: Índice inicial (paginação)
            size: Quantidade de itens
        
        Returns:
            dict/list: Resposta JSON da API ou None se houver erro
        """
        payload = {
            "from": from_index,
            "size": size,
            "requisicoesBusca": [
                {
                    "campo": "tipo",
                    "tipo": "exata",
                    "label": "Tipo",
                    "valor": category
                }
            ],
            "listaOrdenacao": [
                {
                    "campo": "dataFim",
                    "tipoCampo": "long",
                    "tipoOrdenacao": "asc"
                }
            ]
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            if self.debug:
                print(f"    ❌ Erro na requisição: {e}")
            return None
    
    def normalize(self, lots: List[Dict]) -> List[Dict]:
        """
        Normaliza lotes para o formato do Supabase
        ✅ Mapeia campos da API para schema da tabela
        ✅ Extrai dados aninhados
        ✅ Processa valores especiais
        """
        print("\n🔄 NORMALIZANDO DADOS...")
        
        normalized = []
        errors = 0
        
        for lot in lots:
            try:
                item = self._normalize_item(lot)
                if item:
                    normalized.append(item)
                    
                    # Atualiza stats
                    if item.get('has_bids'):
                        self.stats['with_bids'] += 1
                
            except Exception as e:
                errors += 1
                if self.debug:
                    print(f"  ⚠️ Erro ao normalizar lote {lot.get('id')}: {e}")
        
        self.stats['errors'] = errors
        
        print(f"  ✅ Normalizados: {len(normalized)} itens")
        if errors > 0:
            print(f"  ⚠️ Erros: {errors}")
        
        return normalized
    
    def _normalize_item(self, lot: Dict) -> Optional[Dict]:
        """
        Normaliza um único lote
        Mapeia campos da API da Leilo para schema do Supabase
        """
        try:
            # ID único
            lot_id = lot.get('id') or lot.get('lelId')
            if not lot_id:
                return None
            
            external_id = f"leilo_{lot_id}"
            
            # Extrai objetos aninhados
            localizacao = lot.get('localizacao') or {}
            leilao = lot.get('leilao') or {}
            veiculo = lot.get('veiculo') or {}
            valor = lot.get('valor') or {}
            valor_lance = valor.get('lance') or {}
            comitente = lot.get('comitente') or {}
            
            # Monta item normalizado
            item = {
                # Identificação
                'external_id': external_id,
                'lel_id': self._safe_int(lot.get('lelId')),
                'lot_number': self._safe_str(lot.get('numero')),
                'auction_position': self._safe_int(lot.get('posicaoLeilao')),
                
                # Informações básicas
                'title': self._safe_str(lot.get('nome')),
                'category': self._safe_str(lot.get('tipo')),
                'situation': self._safe_str(lot.get('situacao')),
                
                # Localização
                'city': self._safe_str(localizacao.get('cidade')),
                'state': self._safe_str(localizacao.get('estado')),
                'location_full': self._safe_str(localizacao.get('nome')),
                
                # Leilão
                'auction_id': self._safe_str(leilao.get('id')),
                'auction_name': self._safe_str(leilao.get('nome')),
                'auction_date': self._parse_datetime(leilao.get('data')),
                'auction_modality': self._safe_str(leilao.get('modalidade')),
                'auction_payment_date': self._parse_datetime(leilao.get('dataPagamento')),
                
                # Veículo (nullable para outras categorias)
                'vehicle_id': self._safe_str(veiculo.get('id')),
                'brand': self._safe_str(veiculo.get('infocarMarca')),
                'model': self._safe_str(veiculo.get('infocarModelo')),
                'year_model': self._safe_int(veiculo.get('anoModelo')),
                'year_manufacture': self._safe_int(veiculo.get('anoFabricacao')),
                'km': self._safe_int(veiculo.get('km')),
                'market_value': self._parse_numeric(veiculo.get('valorMercado')),
                
                # Valores
                'price_minimum': self._parse_numeric(valor.get('minimo')),
                'price_proposal': self._parse_numeric(valor.get('valorProposta')),
                'bid_increment': self._parse_numeric(valor.get('incremento')),
                'total_fees': self._parse_numeric(valor.get('totalDespesas')),
                'commission_percent': self._parse_numeric(valor.get('comissaoPorcentagem')),
                
                # Lance atual
                'current_bid_value': self._parse_numeric(valor_lance.get('valor')),
                'total_bids': self._safe_int(valor_lance.get('quantidade')),
                'current_bid_date': self._parse_datetime(valor_lance.get('data')),
                
                # Vendedor
                'seller_id': self._safe_int(comitente.get('comId')),
                'seller_name': self._safe_str(comitente.get('nome')),
                
                # Datas
                'end_date': self._parse_datetime(lot.get('dataFim')),
                'change_date': self._parse_datetime(lot.get('dataAlteracao')),
                
                # Mídia
                'photo_count': self._safe_int(lot.get('quantidadeFotos')),
                'video_url': self._safe_str(lot.get('video')),
                
                # Arrays e objetos JSONB
                'photo_urls': self._parse_photo_urls(lot),
                'selos': lot.get('selos') if lot.get('selos') else None,
                'carimbos': lot.get('carimbos') if lot.get('carimbos') else None,
                'arquivos': lot.get('arquivos') if lot.get('arquivos') else None,
                
                # Dados específicos por categoria (JSONB)
                'vehicle_data': veiculo if veiculo and any(veiculo.values()) else None,
                'raw_data': lot,  # JSON completo
                
                # Metadados
                'image_url': self._get_first_image(lot),
                'link': self._build_link(lot),
                'source': 'leilo',
                
                # Flags de controle
                'is_active': True,
                'has_bids': self._safe_int(valor_lance.get('quantidade', 0)) > 0,
                'has_video': bool(lot.get('video')),
                'is_sold': lot.get('situacao') == 'Vendido',
                'is_removed': False,
            }
            
            # Remove None values
            return {k: v for k, v in item.items() if v is not None}
        
        except Exception as e:
            if self.debug:
                print(f"  ⚠️ Erro ao normalizar lote {lot.get('id')}: {e}")
            return None
    
    # ========================================================================
    # MÉTODOS AUXILIARES DE PARSING
    # ========================================================================
    
    def _safe_str(self, value) -> Optional[str]:
        """Converte para string de forma segura"""
        if value is None:
            return None
        try:
            result = str(value).strip()
            return result if result else None
        except:
            return None
    
    def _safe_int(self, value) -> Optional[int]:
        """Converte para int de forma segura"""
        if value is None:
            return None
        try:
            return int(value)
        except:
            return None
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Converte para numeric de forma segura"""
        if value is None:
            return None
        try:
            return float(value)
        except:
            return None
    
    def _parse_datetime(self, value) -> Optional[str]:
        """
        Parse datetime para formato ISO 8601
        Aceita timestamps em milissegundos ou strings ISO
        """
        if not value:
            return None
        
        try:
            # Se for timestamp em milissegundos
            if isinstance(value, (int, float)):
                # Converte de milissegundos para segundos
                timestamp_seconds = value / 1000.0
                dt = datetime.fromtimestamp(timestamp_seconds)
                return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
            
            # Se for string ISO
            if isinstance(value, str):
                value = value.replace('Z', '+00:00')
                if 'T' in value:
                    return value
                
                # Tenta parsear outros formatos
                try:
                    dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
                    return dt.strftime('%Y-%m-%dT%H:%M:%S+00:00')
                except:
                    pass
        except:
            pass
        
        return None
    
    def _parse_photo_urls(self, lot: Dict) -> Optional[List[str]]:
        """Extrai URLs de fotos"""
        photos = lot.get('fotos') or []
        if not photos:
            return None
        
        if isinstance(photos, list):
            urls = []
            for photo in photos:
                if isinstance(photo, str):
                    urls.append(photo)
                elif isinstance(photo, dict):
                    url = photo.get('url') or photo.get('link')
                    if url:
                        urls.append(url)
            return urls if urls else None
        
        return None
    
    def _get_first_image(self, lot: Dict) -> Optional[str]:
        """Retorna primeira imagem (cache)"""
        photos = self._parse_photo_urls(lot)
        if photos and len(photos) > 0:
            return photos[0]
        return None
    
    def _build_link(self, lot: Dict) -> str:
        """Constrói link do lote no site"""
        lot_id = lot.get('id') or lot.get('lelId')
        if lot_id:
            return f"https://leilo.com.br/lote/{lot_id}"
        return "https://leilo.com.br"


async def main():
    """Função principal"""
    print("\n" + "="*70)
    print("🚀 LEILO.COM.BR - SCRAPER COMPLETO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    supabase = None
    
    try:
        # Inicia heartbeat
        if SupabaseClient:
            print("\n💓 Iniciando sistema de heartbeat...")
            supabase = SupabaseClient(
                service_name='leilo_scraper',
                service_type='scraper'
            )
            
            if supabase.test():
                supabase.heartbeat_start(metadata={
                    'scraper': 'leilo',
                    'categories': 7,
                })
        
        # FASE 1: Coleta
        print("\n🔥 FASE 1: COLETANDO DADOS")
        scraper = LeiloScraper(debug=False)
        raw_lots = scraper.scrape()
        
        if not raw_lots:
            print("⚠️ Nenhum lote coletado")
            if supabase:
                supabase.heartbeat_finish(status='warning', final_stats={
                    'items_collected': 0,
                })
            return
        
        # FASE 2: Normalização
        print("\n🔥 FASE 2: NORMALIZANDO DADOS")
        items = scraper.normalize(raw_lots)
        
        print(f"\n✅ Total processado: {len(items)} itens")
        print(f"🔥 Com lances: {scraper.stats['with_bids']}")
        if scraper.stats['errors'] > 0:
            print(f"⚠️  Erros: {scraper.stats['errors']}")
        
        # Salva JSON local
        output_dir = Path(__file__).parent / 'data' / 'normalized'
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = output_dir / f'leilo_{timestamp}.json'
        
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON: {json_file}")
        
        # FASE 3: Upload para Supabase
        if supabase:
            print("\n📤 FASE 3: INSERINDO NO SUPABASE")
            print(f"\n  📤 leilo_items: {len(items)} itens")
            stats = supabase.upsert('leilo_items', items)
            
            print(f"    ✅ Inseridos/Atualizados: {stats['inserted']}")
            if stats.get('duplicates_removed', 0) > 0:
                print(f"    🔄 Duplicatas removidas: {stats['duplicates_removed']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
            
            # Finaliza heartbeat com sucesso
            supabase.heartbeat_success(final_stats={
                'items_collected': len(items),
                'items_inserted': stats['inserted'],
                'items_with_bids': scraper.stats['with_bids'],
                'duplicates_removed': stats.get('duplicates_removed', 0),
                'categories': scraper.stats['by_category'],
            })
    
    except Exception as e:
        print(f"⚠️ Erro crítico: {e}")
        import traceback
        traceback.print_exc()
        
        if supabase:
            supabase.heartbeat_error(str(e)[:500])
    
    finally:
        elapsed = time.time() - start_time
        minutes = int(elapsed // 60)
        seconds = int(elapsed % 60)
        
        print("\n" + "="*70)
        print("📊 ESTATÍSTICAS FINAIS")
        print("="*70)
        print(f"🔵 Leilo.com.br:")
        print(f"  • Total coletado: {scraper.stats['total_scraped']}")
        print(f"  • Com lances: {scraper.stats['with_bids']}")
        print(f"  • Erros: {scraper.stats['errors']}")
        
        print(f"\n📦 Por categoria:")
        for category, count in scraper.stats['by_category'].items():
            print(f"  • {category}: {count}")
        
        print(f"\n⏱️ Duração: {minutes}min {seconds}s")
        print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())