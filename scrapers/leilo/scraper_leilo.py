"""
Script para coletar amostras de TODAS as categorias da Leilo
Analisa diferenças estruturais entre categorias
"""

import requests
import json
import time
from datetime import datetime
from pathlib import Path

# ===========================
# CONFIGURAÇÕES
# ===========================

API_URL = "https://api.leilo.com.br/v1/lote/busca-elastic"

# Categorias a serem analisadas
CATEGORIES = {
    "Carros": "Carros",
    "Motos": "Motos",
    "Pesados": "Pesados",
    "Utilitários": "Utilitários",
    "Imóveis": "Imóveis",
    "Equipamentos": "Equipamentos",
    "Sucatas": "Sucatas"
}

SAMPLES_PER_CATEGORY = 5  # Quantos exemplos coletar de cada categoria
DELAY_BETWEEN_REQUESTS = 1

# Diretórios de saída
OUTPUT_DIR = Path("leilo_data/category_analysis")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

LOGS_FILE = OUTPUT_DIR / "category_analysis.log"

# ===========================
# FUNÇÕES AUXILIARES
# ===========================

def log(message):
    """Registra mensagem com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    
    with open(LOGS_FILE, "a", encoding="utf-8") as f:
        f.write(log_msg + "\n")

def make_request(category, from_index=0, size=5):
    """Faz requisição POST para a API da Leilo"""
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "content-type": "application/json",
        "origin": "https://leilo.com.br",
        "referer": "https://leilo.com.br/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
    }
    
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
        response = requests.post(API_URL, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log(f"❌ Erro na requisição: {e}")
        return None

def extract_all_keys(obj, prefix="", keys_set=None):
    """Extrai recursivamente todas as chaves de um objeto JSON"""
    if keys_set is None:
        keys_set = set()
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            keys_set.add(full_key)
            
            if isinstance(value, dict):
                extract_all_keys(value, full_key, keys_set)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                extract_all_keys(value[0], full_key, keys_set)
    
    return keys_set

def analyze_category_structure(category_name, lotes):
    """Analisa a estrutura de uma categoria"""
    log(f"\n🔍 Analisando estrutura da categoria: {category_name}")
    
    all_keys = set()
    field_types = {}
    field_examples = {}
    
    for lote in lotes:
        # Extrair todas as chaves
        keys = extract_all_keys(lote)
        all_keys.update(keys)
        
        # Analisar tipos e exemplos
        for key in keys:
            parts = key.split('.')
            value = lote
            
            try:
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        break
                
                # Registrar tipo
                value_type = type(value).__name__
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(value_type)
                
                # Registrar exemplo (se não for muito grande)
                if key not in field_examples and value is not None:
                    if isinstance(value, (str, int, float, bool)):
                        field_examples[key] = value
                    elif isinstance(value, list) and len(value) <= 3:
                        field_examples[key] = value
                    elif isinstance(value, dict) and len(value) <= 3:
                        field_examples[key] = value
                        
            except:
                pass
    
    log(f"  ✅ Total de campos encontrados: {len(all_keys)}")
    
    return {
        "category": category_name,
        "total_samples": len(lotes),
        "total_fields": len(all_keys),
        "fields": sorted(all_keys),
        "field_types": {k: list(v) for k, v in field_types.items()},
        "field_examples": field_examples
    }

# ===========================
# FUNÇÃO PRINCIPAL
# ===========================

def collect_samples():
    """Coleta amostras de todas as categorias"""
    log("=" * 60)
    log("🔍 COLETA DE AMOSTRAS - TODAS AS CATEGORIAS")
    log("=" * 60)
    
    all_samples = {}
    all_analysis = {}
    
    for category_key, category_name in CATEGORIES.items():
        log(f"\n{'=' * 60}")
        log(f"📦 CATEGORIA: {category_name}")
        log(f"{'=' * 60}")
        
        # Fazer requisição
        log(f"🔍 Buscando {SAMPLES_PER_CATEGORY} amostras...")
        response_data = make_request(category_name, from_index=0, size=SAMPLES_PER_CATEGORY)
        
        if response_data is None:
            log(f"❌ Falha ao coletar {category_name}")
            continue
        
        # Extrair lotes
        lotes = []
        if isinstance(response_data, dict):
            lotes = response_data.get("content", [])
        elif isinstance(response_data, list):
            lotes = response_data
        
        if not lotes:
            log(f"⚠️ Nenhum lote encontrado para {category_name}")
            continue
        
        log(f"✅ Coletados {len(lotes)} lotes")
        
        # Salvar amostras
        category_file = OUTPUT_DIR / f"samples_{category_key.lower()}.json"
        with open(category_file, "w", encoding="utf-8") as f:
            json.dump(lotes, f, ensure_ascii=False, indent=2)
        log(f"💾 Amostras salvas: {category_file}")
        
        # Analisar estrutura
        analysis = analyze_category_structure(category_name, lotes)
        
        # Salvar análise
        analysis_file = OUTPUT_DIR / f"analysis_{category_key.lower()}.json"
        with open(analysis_file, "w", encoding="utf-8") as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2)
        log(f"📊 Análise salva: {analysis_file}")
        
        all_samples[category_key] = lotes
        all_analysis[category_key] = analysis
        
        # Aguardar antes da próxima requisição
        if category_key != list(CATEGORIES.keys())[-1]:
            log(f"⏳ Aguardando {DELAY_BETWEEN_REQUESTS}s...")
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # ===========================
    # ANÁLISE COMPARATIVA
    # ===========================
    
    log("\n" + "=" * 60)
    log("📊 ANÁLISE COMPARATIVA ENTRE CATEGORIAS")
    log("=" * 60)
    
    # Campos comuns a todas as categorias
    if all_analysis:
        all_fields_by_category = {
            cat: set(analysis["fields"]) 
            for cat, analysis in all_analysis.items()
        }
        
        # Campos que aparecem em TODAS as categorias
        common_fields = set.intersection(*all_fields_by_category.values())
        log(f"\n✅ Campos comuns a TODAS as categorias ({len(common_fields)}):")
        for field in sorted(common_fields):
            log(f"  - {field}")
        
        # Campos únicos por categoria
        log(f"\n🔍 Campos únicos por categoria:")
        for cat, fields in all_fields_by_category.items():
            unique_fields = fields - common_fields
            if unique_fields:
                log(f"\n  📦 {cat} ({len(unique_fields)} campos únicos):")
                for field in sorted(unique_fields):
                    log(f"    - {field}")
        
        # Campos que aparecem em PELO MENOS uma categoria
        all_possible_fields = set.union(*all_fields_by_category.values())
        log(f"\n📋 Total de campos únicos (todas categorias): {len(all_possible_fields)}")
        
        # Salvar comparação
        comparison = {
            "common_fields": sorted(common_fields),
            "fields_by_category": {
                cat: sorted(fields) for cat, fields in all_fields_by_category.items()
            },
            "unique_fields_by_category": {
                cat: sorted(fields - common_fields) 
                for cat, fields in all_fields_by_category.items()
            },
            "all_possible_fields": sorted(all_possible_fields),
            "statistics": {
                "total_categories": len(all_analysis),
                "total_unique_fields": len(all_possible_fields),
                "total_common_fields": len(common_fields),
                "fields_per_category": {
                    cat: len(fields) for cat, fields in all_fields_by_category.items()
                }
            }
        }
        
        comparison_file = OUTPUT_DIR / "category_comparison.json"
        with open(comparison_file, "w", encoding="utf-8") as f:
            json.dump(comparison, f, ensure_ascii=False, indent=2)
        log(f"\n💾 Comparação salva: {comparison_file}")
        
        # Identificar campos específicos de veículos vs imóveis
        vehicle_categories = {"Carros", "Motos", "Pesados", "Utilitários", "Sucatas"}
        vehicle_fields = set.union(*[
            all_fields_by_category[cat] 
            for cat in vehicle_categories 
            if cat in all_fields_by_category
        ])
        
        if "Imóveis" in all_fields_by_category:
            property_fields = all_fields_by_category["Imóveis"]
            
            vehicle_only = vehicle_fields - property_fields
            property_only = property_fields - vehicle_fields
            
            log(f"\n🚗 Campos específicos de VEÍCULOS ({len(vehicle_only)}):")
            for field in sorted(vehicle_only):
                if 'veiculo' in field.lower() or 'km' in field.lower() or 'ano' in field.lower():
                    log(f"  - {field}")
            
            log(f"\n🏠 Campos específicos de IMÓVEIS ({len(property_only)}):")
            for field in sorted(property_only):
                log(f"  - {field}")
    
    # ===========================
    # RESUMO FINAL
    # ===========================
    
    log("\n" + "=" * 60)
    log("📊 RESUMO FINAL")
    log("=" * 60)
    
    for category, analysis in all_analysis.items():
        log(f"\n📦 {category}:")
        log(f"  - Amostras coletadas: {analysis['total_samples']}")
        log(f"  - Campos únicos: {analysis['total_fields']}")
    
    log(f"\n📁 Arquivos gerados:")
    log(f"  - Amostras: {OUTPUT_DIR}/samples_*.json")
    log(f"  - Análises: {OUTPUT_DIR}/analysis_*.json")
    log(f"  - Comparação: {OUTPUT_DIR}/category_comparison.json")
    log(f"  - Logs: {LOGS_FILE}")
    
    log("\n" + "=" * 60)
    log("✅ ANÁLISE CONCLUÍDA!")
    log("=" * 60)
    
    return all_samples, all_analysis, comparison

# ===========================
# GERAÇÃO DE SCHEMA UNIFICADO
# ===========================

def generate_unified_schema(comparison):
    """Gera sugestão de schema SQL unificado"""
    log("\n" + "=" * 60)
    log("🏗️ GERANDO SCHEMA UNIFICADO")
    log("=" * 60)
    
    common_fields = set(comparison["common_fields"])
    all_fields = set(comparison["all_possible_fields"])
    
    # Campos que devem ser colunas diretas (comuns + importantes)
    direct_columns = common_fields.copy()
    
    # Adicionar campos importantes mesmo que não sejam comuns
    important_fields = {
        "veiculo", "veiculo.infocarMarca", "veiculo.infocarModelo", 
        "veiculo.km", "veiculo.anoModelo", "veiculo.anoFabricacao"
    }
    direct_columns.update(important_fields & all_fields)
    
    log(f"\n✅ Campos mapeados como colunas diretas: {len(direct_columns)}")
    log(f"📦 Campos armazenados em JSONB: {len(all_fields - direct_columns)}")
    
    # Categorizar campos
    schema_suggestion = {
        "common_fields": sorted(common_fields),
        "direct_columns": sorted(direct_columns),
        "jsonb_fields": sorted(all_fields - direct_columns),
        "vehicle_specific": sorted([
            f for f in all_fields 
            if 'veiculo' in f.lower() or 'km' in f.lower()
        ]),
        "property_specific": sorted([
            f for f in all_fields 
            if 'imovel' in f.lower() or 'm2' in f.lower() or 'quarto' in f.lower()
        ])
    }
    
    schema_file = OUTPUT_DIR / "unified_schema_suggestion.json"
    with open(schema_file, "w", encoding="utf-8") as f:
        json.dump(schema_suggestion, f, ensure_ascii=False, indent=2)
    
    log(f"💾 Sugestão de schema salva: {schema_file}")
    
    return schema_suggestion

# ===========================
# EXECUÇÃO
# ===========================

if __name__ == "__main__":
    try:
        samples, analysis, comparison = collect_samples()
        schema_suggestion = generate_unified_schema(comparison)
        
        print("\n" + "=" * 60)
        print("✅ PRÓXIMOS PASSOS:")
        print("=" * 60)
        print("1. Revise os arquivos em: leilo_data/category_analysis/")
        print("2. Analise as diferenças entre categorias")
        print("3. Decida: tabela única ou separar veículos/imóveis?")
        print("4. Gere o SQL unificado baseado na análise")
        print("=" * 60)
        
    except KeyboardInterrupt:
        log("\n⚠️ Interrompido pelo usuário")
    except Exception as e:
        log(f"\n❌ Erro: {e}")
        import traceback
        traceback.print_exc()