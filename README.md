# Desafio - Intuitive Care

## Tecnologias
> Python

> POSTGRESql

> VueJs

## Importante !

O README está dividido em sessões de acordo com o PDF enviado por email, justificativas de determinadas
decisões podem ser encontradas nas sessões à qual pertence a etapa;

#### Como executar:

O padrão é sempre o mesmo, em todos os desafios:

```bash

# 1. Entre na pasta do desafio
cd desafio{numero_desafio} 

# 2. Crie o ambiente virtual
python3 -m venv .venv

# 3. Ative o ambiente virtual
source .venv/bin/activate #(para SO baseado em linux)

# 4. Instale as dependências
pip install -r requirements.txt

# 5. Execute o projeto
python main.py
 
 ``` 

## Estrutura do monorepo:

```
.
├── desafio1
│   ├── main.py
│   ├── output
│   ├── pyrightconfig.json
│   ├── raw
│   ├── requirements.txt
│   └── src
├── desafio2
│   ├── main.py
│   ├── output
│   ├── pyrightconfig.json
│   ├── requirements.txt
│   └── src
└── README.md
```



# Arquitetura e Decisões Técnicas

## **1.0 – Pipeline ETL (Desafio 1)**

### **Crawling Resiliente a Estruturas Variáveis**
- **Decisão**: Implementar crawler que navega pela estrutura de diretórios da ANS (`YYYY/QQ/`) e identifica arquivos por padrão de nome, não por caminho fixo.
- **Justificativa**: A ANS pode alterar periodicamente a organização dos arquivos; abordagem baseada em regex é mais robusta que paths hardcoded.
- **Prós**: Funciona mesmo com mudanças na estrutura de diretórios.  
- **Contras**: Pode capturar arquivos irrelevantes se o padrão for muito genérico.

### **Processamento Incremental (Chunking)**
- **Decisão**: Processar arquivos em chunks de 150k linhas em vez de carregar tudo em memória.
- **Justificativa**: Arquivos trimestrais têm ~250k linhas cada; chunking evita estouro de RAM em máquinas com recursos limitados.
- **Prós**: Escalável; opera com footprint de memória constante.  
- **Contras**: Complexidade adicional na lógica de streaming.

### **Filtro por Código Contábil "41"**
- **Decisão**: Selecionar apenas registros onde `CD_CONTA_CONTABIL` começa com `"41"`.
- **Justificativa**: Requisito explícito do desafio: "Despesas com Eventos/Sinistros" correspondem ao Grupo 41 do plano de contas da ANS.
- **Prós**: Foco exato no escopo solicitado; redução de 95%+ do volume de dados. 

<img width="993" height="323" alt="image" src="https://github.com/user-attachments/assets/6ed6ff86-bef1-4dff-93d9-12cd34763caf" />

### **Consolidação por Entidade e Período**
- **Decisão**: Agrupar por `CNPJ + Ano + Trimestre` e somar `ValorDespesas`.
- **Justificativa**: Múltiplos registros por operadora/trimestre devem ser consolidados em um único valor.
- **Prós**: Saída limpa e analítica conforme exigido.  
- **Contras**: Perda de granularidade de contas contábeis individuais.

### **Tratamento de Inconsistências**

#### **CNPJs Duplicados com Razões Sociais Diferentes**
- **Decisão**: Manter registro mais recente do CADOP (baseado em `Data_Registro_ANS`).
- **Justificativa**: Garante uso da razão social atualizada, evitando conflitos históricos.
- **Prós**: Consistência temporal; alinhamento com realidade jurídica atual.  
- **Contras**: Pode ocultar histórico de sucessões ou fusões.

#### **Valores Zerados ou Negativos**
- **Decisão**: Preservar todos os valores sem filtragem.
- **Justificativa**: Valores negativos representam ajustes ou recuperações; zerados indicam inatividade — ambos são informações válidas.
- **Prós**: Integridade financeira completa.  
- **Contras**: Requer tratamento específico em análises que consideram apenas despesas positivas.

#### **Datas com Formatos Inconsistentes**
- **Decisão**: Normalizar com `pd.to_datetime(format='mixed', errors='coerce')` e remover registros com `NaT`.
- **Justificativa**: Garante extração correta de ano/trimestre para consolidação.
- **Prós**: Robustez contra múltiplos formatos de data.  
- **Contras**: Potencial perda de registros com datas irrecuperáveis.

---

## **2.0 – Validação e Enriquecimento**

### **2.1. Validação com Flags (Não Filtragem)**
- **Decisão**: Manter todos os registros e adicionar flag `RegistroCNPJValido`.
- **Justificativa**: Preservação integral dos dados financeiros para auditoria.
- **Prós**: Transparência na qualidade dos dados; análise segmentada possível.  
- **Contras**: Dataset final inclui registros potencialmente inválidos.

### **2.2. Left Join com CADOP**
- **Decisão**: Enriquecer com `UF` via left join por CNPJ; preencher não-matches com `"XX"`.
- **Justificativa**: Volume pequeno (~1.5k registros); descarte seria perda crítica de informação.
- **Prós**: 100% dos dados financeiros preservados; enriquecimento geográfico habilitado.  
- **Contras**: Requer validação adicional para uso em relatórios oficiais.

### **2.3. Agregação por RazaoSocial + UF**
- **Decisão**: Agrupar por `RazaoSocial + UF` e calcular total, média trimestral e desvio padrão.
- **Justificativa**: Volume reduzido (< 2k registros); pandas groupby é otimizado e suficiente.
- **Prós**: Simplicidade; alinhamento com requisitos analíticos geográficos.  
- **Contras**: Não escalável para volumes massivos (ex: > 1M registros).
---

Devido ao tempo de aprendizado dedicado às ferramentas de manipulação de dados e à complexidade da estrutura da ANS, as etapas de Banco de Dados e API não foram incluídas nesta entrega. Optei por consolidar um código funcional e limpo nas primeiras fases em vez de entregar implementações parciais sem a devida maturidade técnica.
