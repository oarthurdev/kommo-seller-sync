#!/usr/bin/env python3
"""
Script de teste para validar a lógica da regra total_leads
"""

import pandas as pd
import json
from libs.supabase_db import SupabaseClient

def test_total_leads_logic():
    """Teste da lógica da regra total_leads"""
    
    # Dados de teste simulando leads com custom_fields_values
    leads_data = [
        {
            'id': '1',
            'custom_fields_values': json.dumps([
                {
                    'field_id': 1041669,
                    'field_name': 'Corretor responsável',
                    'field_code': None,
                    'field_type': 'select',
                    'values': [{'value': 'Maicon', 'enum_id': 821251, 'enum_code': None}]
                }
            ])
        },
        {
            'id': '2',
            'custom_fields_values': json.dumps([
                {
                    'field_id': 1041669,
                    'field_name': 'Corretor responsável',
                    'field_code': None,
                    'field_type': 'select',
                    'values': [{'value': 'João', 'enum_id': 821252, 'enum_code': None}]
                }
            ])
        },
        {
            'id': '3',
            'custom_fields_values': json.dumps([
                {
                    'field_id': 1041669,
                    'field_name': 'Corretor responsável',
                    'field_code': None,
                    'field_type': 'select',
                    'values': [{'value': 'Maicon', 'enum_id': 821251, 'enum_code': None}]
                }
            ])
        },
        {
            'id': '4',
            'custom_fields_values': json.dumps([
                {
                    'field_id': 1041695,
                    'field_name': 'Sem contato',
                    'field_code': None,
                    'field_type': 'checkbox',
                    'values': [{'value': True}]
                }
            ])  # Este lead não tem corretor responsável
        },
        {
            'id': '5',
            'custom_fields_values': ''  # Campo vazio
        }
    ]
    
    # Converter para DataFrame
    all_leads = pd.DataFrame(leads_data)
    
    # Testar a lógica para o corretor "Maicon"
    broker_name = "Maicon"
    total_count = 0
    
    print(f"Testando contagem para o corretor: {broker_name}")
    print(f"Total de leads para processar: {len(all_leads)}")
    
    # Lógica extraída da implementação
    for idx, lead in all_leads.iterrows():
        try:
            custom_fields_str = lead.get('custom_fields_values', '')
            
            # Verificar se o campo não está vazio
            if not custom_fields_str or pd.isna(custom_fields_str):
                print(f"Lead {lead['id']}: Campo custom_fields_values vazio ou nulo")
                continue
                
            # Parse do JSON
            custom_fields = json.loads(custom_fields_str)
            
            # Verificar se é uma lista
            if not isinstance(custom_fields, list):
                print(f"Lead {lead['id']}: custom_fields_values não é uma lista")
                continue
                
            # Procurar pelo campo "Corretor responsável"
            for field in custom_fields:
                if (isinstance(field, dict) and 
                    field.get('field_name') == 'Corretor responsável' and
                    'values' in field and 
                    isinstance(field['values'], list) and
                    len(field['values']) > 0):
                    
                    # Extrair o valor do corretor
                    corretor_value = field['values'][0].get('value', '')
                    print(f"Lead {lead['id']}: Corretor encontrado = '{corretor_value}'")
                    
                    # Comparar com o nome do broker atual
                    if corretor_value == broker_name:
                        total_count += 1
                        print(f"Lead {lead['id']}: ✅ Contabilizado para {broker_name}")
                        break  # Sair do loop de campos
                    else:
                        print(f"Lead {lead['id']}: ❌ Não contabilizado (corretor diferente)")
                        break
            else:
                print(f"Lead {lead['id']}: Campo 'Corretor responsável' não encontrado")
                        
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Lead {lead['id']}: Erro ao processar JSON - {e}")
            continue
    
    print(f"\n🎯 Resultado final: {total_count} leads para o corretor {broker_name}")
    print(f"✅ Resultado esperado: 2 leads (IDs 1 e 3)")
    
    # Validar resultado
    expected_count = 2
    if total_count == expected_count:
        print("✅ TESTE PASSOU! A lógica está funcionando corretamente.")
        return True
    else:
        print(f"❌ TESTE FALHOU! Esperado: {expected_count}, Obtido: {total_count}")
        return False

if __name__ == "__main__":
    test_total_leads_logic()