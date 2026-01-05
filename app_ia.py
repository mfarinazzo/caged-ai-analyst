import streamlit as st
import streamlit.components.v1 as components
from google import genai
from databricks import sql
import datetime
import pandas as pd
import io
import time
import random
import os
import json

# --- CONFIGURAÇÕES DE CREDENCIAIS ---
def _get_secret(name: str, default: str = "") -> str:
    return (
        st.secrets.get(name)
        if hasattr(st, "secrets") and name in st.secrets
        else os.getenv(name, default)
    )

DATABRICKS_HOST = _get_secret("DATABRICKS_HOST", "")
DATABRICKS_HTTP_PATH = _get_secret("DATABRICKS_HTTP_PATH", "")
DATABRICKS_TOKEN = _get_secret("DATABRICKS_TOKEN", "")
GEMINI_API_KEY = _get_secret("GEMINI_API_KEY", "")

# Configuração da Página
st.set_page_config(page_title="CAGED AI Analyst", layout="wide")

# Inicialização do Cliente Gemini
client = genai.Client(api_key=GEMINI_API_KEY)

# --- DEFINIÇÃO DO SCHEMA ---
TABLE_SCHEMA = """
Tabela: cageddatabricks.banco_caged.tabela_silver
Colunas:
- ano (INT): Ano da movimentação (ex: 2024, 2025)
- mes (INT): Mês da movimentação (1 a 12)
- id_municipio (INT): Código IBGE do município (6 dígitos, ex: 350950 para Campinas)
- id_cbo (STRING): Código da ocupação (ex: '212405'). Para TI use LIKE '2124%'
- id_raca (INT)
- saldo_movimentacao (INT): 1 para Admissão, -1 para Desligamento
- tipo_movimentacao (STRING): Descrição do tipo (Admissão/Desligamento)
- salario_mensal_final (DOUBLE): Valor do salário
- id_sexo (INT): 1-Masculino, 3-Feminino
- idade (INT): Idade do trabalhador
"""

# --- FUNÇÕES AUXILIARES ---

CONTEXTO_AUXILIAR = """
--- DICIONÁRIO DE DADOS (REFERÊNCIA RÁPIDA) ---

1. MAPEAMENTO DE CBOs (OCUPAÇÕES DE TI):
   - Use LIKE '2124%' para: Analistas de TI, Desenvolvedores, Engenheiros de SW.
   - 2124-05: Analista de desenvolvimento de sistemas
   - 2124-10: Analista de redes e de comunicação de dados
   - 2124-20: Analista de suporte computacional
   - 2123-05: Administrador de banco de dados (DBA)
   - 2122-05: Engenheiro de aplicativos em computação
   - 3171-10: Programador de sistemas de informação
   - 3171-05: Programador de internet / Web Design
   - 1425-05: Gerente de TI

2. MAPEAMENTO DE MUNICÍPIOS (CÓDIGO 6 DÍGITOS):
   - 350950: Campinas (SP)
   - 351907: Hortolândia (SP)
   - 355240: Sumaré (SP)
   - 353650: Paulínia (SP)
   - 352050: Indaiatuba (SP)
   - 355030: São Paulo (Capital)
   - 330455: Rio de Janeiro (Capital)
   - 310620: Belo Horizonte (Capital)

3. CÓDIGOS DE CATEGORIA:
   - id_sexo: 1=Masculino, 3=Feminino
   - raca_cor: 1=Branca, 2=Preta, 3=Parda, 4=Amarela, 5=Indígena
   - tipo_movimentacao: Tente filtrar por saldo (1=Admissão, -1=Desligamento) se disponível, ou use os códigos: 10, 20, 25, 31, 32, 35, 40, 43, 45, 50, 60, 90, 98.
"""

def executar_com_retry(funcao_chamada, max_tentativas=3):
    """Executa função com retry automático para erro 429 (Rate Limit)."""
    for tentativa in range(max_tentativas):
        try:
            return funcao_chamada()
        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                if tentativa < max_tentativas - 1:
                    time.sleep((2 ** tentativa) + random.uniform(0, 1))
                    continue
            raise e

def render_highcharts_dinamico(config_grafico, dados_formatados):
    """
    Renderiza Highcharts suportando MÚLTIPLAS SÉRIES (Comparativos).
    """
    chart_type = config_grafico.get('tipo', 'spline') # Padrão spline (linha suave)
    title_text = config_grafico.get('titulo_grafico', 'Análise de Mercado')
    y_axis_text = config_grafico.get('titulo_eixo_y', 'Valor')
    
    # Agora esperamos 'categories' para o Eixo X e 'series' para os dados
    categories = dados_formatados.get('categories', [])
    series_data = dados_formatados.get('series', [])

    # Serialização
    categories_json = json.dumps(categories)
    series_json = json.dumps(series_data)

    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <script src="https://code.highcharts.com/highcharts.js"></script>
        <script src="https://code.highcharts.com/modules/exporting.js"></script>
        <script src="https://code.highcharts.com/modules/export-data.js"></script>
        <script src="https://code.highcharts.com/modules/accessibility.js"></script>
        <style>
            #container {{
                width: 100%;
                height: 400px;
                background-color: transparent;
            }}
        </style>
    </head>
    <body>
        <div id="container"></div>
        <script>
            document.addEventListener('DOMContentLoaded', function () {{
                Highcharts.chart('container', {{
                    chart: {{
                        type: '{chart_type}',
                        backgroundColor: 'transparent',
                        style: {{ fontFamily: 'sans-serif' }}
                    }},
                    title: {{
                        text: '{title_text}',
                        style: {{ color: '#FFFFFF', fontWeight: 'bold' }}
                    }},
                    subtitle: {{
                        text: 'Fonte: CAGED',
                        style: {{ color: '#CCCCCC' }}
                    }},
                    xAxis: {{
                        categories: {categories_json},
                        lineColor: '#FFFFFF',
                        tickColor: '#FFFFFF',
                        labels: {{ style: {{ color: '#FFFFFF' }} }},
                        crosshair: true
                    }},
                    yAxis: {{
                        title: {{
                            text: '{y_axis_text}',
                            style: {{ color: '#FFFFFF' }}
                        }},
                        gridLineColor: '#444444',
                        labels: {{ style: {{ color: '#FFFFFF' }} }}
                    }},
                    legend: {{
                        itemStyle: {{ color: '#FFFFFF' }},
                        itemHoverStyle: {{ color: '#AAAAAA' }}
                    }},
                    tooltip: {{
                        shared: true,
                        backgroundColor: 'rgba(0, 0, 0, 0.85)',
                        style: {{ color: '#FFFFFF' }},
                        valueDecimals: 2,
                        headerFormat: '<span style="font-size: 10px">{{point.key}}</span><br/>'
                    }},
                    credits: {{ enabled: false }},
                    exporting: {{
                        enabled: true,
                        buttons: {{
                            contextButton: {{
                                symbolStroke: '#FFFFFF',
                                theme: {{ fill: '#333333' }}
                            }}
                        }}
                    }},
                    plotOptions: {{
                        spline: {{ marker: {{ radius: 4, lineWidth: 1 }} }},
                        column: {{ borderRadius: 0, borderColor: 'transparent' }}
                    }},
                    // A MÁGICA ACONTECE AQUI: Injetamos as séries dinâmicas da IA
                    series: {series_json}
                }});
            }});
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=420)
# --- INTELIGÊNCIA ARTIFICIAL (AGENTE DE MERCADO) ---

def gemini_text_to_sql(user_question):
    """Gera query SQL via Gemini."""
    prompt = f"""
    Você é um Especialista em SQL Databricks.
    Schema: {TABLE_SCHEMA}
    Pergunta: {user_question}
    
    Regras Críticas:
    1. Retorne APENAS o SQL puro. Sem markdown, sem explicações.
    2. Use LIMIT 1000 se não houver agregação explícita.
    3. Para evolução temporal, agrupe por ano e mês.
    4. Filtros comuns: TI (id_cbo LIKE '2124%'), Campinas (id_municipio = 350950).
    """
    response = executar_com_retry(lambda: client.models.generate_content(model='gemini-3-flash-preview', contents=prompt))
    return response.text.replace("```sql", "").replace("```", "").strip()

def consultar_agente_de_mercado(pergunta_usuario, df):
    """
    Envia os dados para o Agente com regras estritas de formatação de texto limpo.
    """
    dados_str = df.to_csv(index=False) 

    system_instruction = """
    Você é um Analista de Dados Sênior (CAGED).
    
    REGRAS VISUAIS E DE FORMATAÇÃO (CRÍTICO):
    1. Visualização: Gere múltiplas séries ('series') se houver comparação (ex: Gênero, Faixa Etária). Defina cores distintas.
    2. Texto da Análise:
       - Comece ESTRITAMENTE com "Prezado,".
       - TEXTO LIMPO: NÃO use crases (`), negrito (**) ou itálico (*) em valores monetários, datas ou números. Escreva apenas o texto puro. Ex: "R$ 5.000,00" e não "`R$ 5.000,00`".
       - O texto deve ser formal, justificado e dividido em parágrafos claros.
    
    Auxiliar: {CONTEXTO_AUXILIAR}
    (Nunca retorne números ao cliente, sempre veja qual o valor daquele número e escreva ele por extenso no texto)
    
    ESTRUTURA DE RESPOSTA (JSON):
    {
        "tipo_visualizacao": "grafico" | "indicador_unico" | "texto",
        "configuracao_grafico": {
            "tipo": "spline" | "column" | "area",
            "titulo_eixo_x": "Mês",
            "titulo_eixo_y": "Salário (R$)",
            "titulo_grafico": "Título Profissional"
        },
        "dados_formatados": {
            "categories": ["Jan", "Fev"], 
            "series": [
                { "name": "Série A", "data": [10, 20], "color": "#00BFFF" },
                { "name": "Série B", "data": [15, 25], "color": "#FF69B4" }
            ]
        },
        "analise_executiva": "Texto corrido, formal e SEM formatação markdown nos números...",
        "resumo_lateral_bullets": [
            "Ponto 1: Explicação",
            "Ponto 2: Explicação"
        ]
    }
    """

    prompt_completo = f"""
    {system_instruction}
    
    ---
    DADOS:
    {dados_str}
    
    PERGUNTA:
    {pergunta_usuario}
    """

    try:
        response = executar_com_retry(lambda: client.models.generate_content(
            model='gemini-3-flash-preview', 
            contents=prompt_completo,
            config={'response_mime_type': 'application/json'}
        ))
        return json.loads(response.text)
    except Exception as e:
        st.error(f"Erro no Agente: {e}")
        return None
# --- BACKEND DATABRICKS ---

def get_databricks_connection():
    if not DATABRICKS_TOKEN:
        raise ValueError("Token do Databricks não configurado.")
    try:
        conn = sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
            timeout=30,
        )
    except TypeError:
        conn = sql.connect(
            server_hostname=DATABRICKS_HOST,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
    return conn

# --- INTERFACE ---

def reset_state():
    for key in ['dados_caged', 'resposta_agente', 'sql_gerado']:
        if key in st.session_state:
            del st.session_state[key]

st.title("Plataforma de Inteligência CAGED")
st.markdown("Monitoramento de Mercado de Trabalho - Agente Autônomo")

user_input = st.text_input("Pergunta de Negócio:", 
                          placeholder="Ex: Qual a tendência salarial de Desenvolvedores em Campinas?",
                          on_change=reset_state)

if st.button("Executar Análise"):
    if not user_input:
        st.warning("Insira uma pergunta para iniciar.")
    else:
        try:
            # 1. SQL
            with st.spinner("Traduzindo pergunta para SQL..."):
                sql_query = gemini_text_to_sql(user_input)
                st.session_state['sql_gerado'] = sql_query
            
            # 2. Databricks
            with st.status("Processando dados no Data Lake...", expanded=True) as status:
                conn = get_databricks_connection()
                df = pd.read_sql(sql_query, conn)
                conn.close()
                status.update(label="Dados recuperados com sucesso", state="complete", expanded=False)
            
            st.session_state['dados_caged'] = df

            # 3. Agente AI
            if not df.empty:
                with st.spinner("Gerando análise executiva..."):
                    resposta_agente = consultar_agente_de_mercado(user_input, df)
                    st.session_state['resposta_agente'] = resposta_agente
            else:
                st.warning("A consulta não retornou dados.")

        except Exception as e:
            st.error(f"Erro técnico: {e}")

# --- RENDERIZAÇÃO FINAL ---
if 'dados_caged' in st.session_state and 'resposta_agente' in st.session_state:
    df_result = st.session_state['dados_caged']
    agente_data = st.session_state['resposta_agente']
    
    if 'sql_gerado' in st.session_state:
        with st.expander("Metadados Técnicos (Query Executada)"):
            st.code(st.session_state['sql_gerado'], language="sql")

    if agente_data:
        tipo_viz = agente_data.get('tipo_visualizacao', 'texto')
        
        # LIMPEZA PREVENTIVA: Remove crases para evitar o fundo verde (formato de código)
        texto_analise = agente_data.get('analise_executiva', '').replace("`", "").replace("```", "")
        bullets = agente_data.get('resumo_lateral_bullets', [])

        st.markdown("## 🤖 Análise Executiva")
        st.markdown("---")

        # 1. Texto Completo Justificado e Limpo
        st.markdown(f"""
        <div style="
            text-align: justify; 
            font-family: 'Source Sans Pro', sans-serif; 
            font-size: 1.05rem; 
            color: #E0E0E0; 
            line-height: 1.6; 
            margin-bottom: 25px;">
            {texto_analise.replace(chr(10), '<br>')} 
        </div>
        """, unsafe_allow_html=True)
        # O replace(chr(10), '<br>') garante que as quebras de linha da IA virem parágrafos no HTML

        # 2. Visualização Dividida
        if tipo_viz == 'grafico':
            col_graph, col_summary = st.columns([3, 1])
            
            with col_graph:
                render_highcharts_dinamico(
                    agente_data.get('configuracao_grafico', {}),
                    agente_data.get('dados_formatados', {})
                )
            
            with col_summary:
                st.markdown("### Insights")
                
                # Renderiza lista HTML com indentação correta
                li_items = "".join([f"<li style='margin-bottom: 10px; margin-left: 15px;'>{item}</li>" for item in bullets])
                
                st.markdown(f"""
                <div style="
                    background-color: #262730; 
                    padding: 20px; 
                    border-radius: 8px; 
                    border: 1px solid #444; 
                    box-shadow: 2px 2px 5px rgba(0,0,0,0.3);">
                    <ul style="
                        list-style-type: disc; 
                        padding: 0; 
                        margin: 0; 
                        color: #FFFFFF; 
                        font-size: 0.95rem;">
                        {li_items}
                    </ul>
                </div>
                """, unsafe_allow_html=True)

        # 3. Indicador Único
        elif tipo_viz == 'indicador_unico':
            st.metric(label="Resultado", value=agente_data.get('dados_formatados', {}).get('valores', [0])[0])

        st.divider()

        # Download Dados
        st.subheader("Dados Fonte")
        st.dataframe(df_result, use_container_width=True, height=200)
        
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            df_result.to_excel(writer, index=False, sheet_name='Analise')
        buffer.seek(0)
            
        st.download_button(
            label="Baixar Planilha (.xlsx)",
            data=buffer,
            file_name="relatorio_caged.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        # mostrar popover do código SQL gerado pela IA
        if 'sql_gerado' in st.session_state:
                with st.popover("Mostrar SQL Utilizado"):
                    st.code(st.session_state['sql_gerado'], language="sql")
                    st.caption("Query gerada pelo Gemini e executada no Databricks.")

        # 2. Rodapé com Data e Hora
        data_atual = datetime.datetime.now().strftime("%d/%m/%Y às %H:%M")

        st.markdown(f"""
        ---
        <div style="text-align: right;">
            <small style="color:gray;">
            Relatório gerado em {data_atual}.
            </small>
        </div>
        """, unsafe_allow_html=True)