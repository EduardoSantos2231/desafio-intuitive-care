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
cd desafio1

# 2. Crie o ambiente virtual
python3 -m venv .venv

# 3. Ative o ambiente virtual
source .venv/bin/activate

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
│   ├── pyrightconfig.json
│   ├── requirements.txt
│   └── src
│       ├── ingestion
│       │   ├── crawler.py
│       │   ├── downloader.py
│       │   └── zip_extractor.py
│       ├── __init__.py
│       ├── processing
│       │   ├── base_processor.py
│       │   ├── csv_processor.py
│       │   ├── factory_processor.py
│       │   └── txt_processor.py
│       └── transformation
│           ├── accounting_transformer.py
│           ├── cadop_cleaner.py
│           ├── expense_calculator.py
│           ├── output_manager.py
│           └── pipeline.py
└── README.md
```

## Arquitetura e Decisões Técnicas (Etapa 1)

Abaixo constam as documentações de cada etapa e decisões tomadas de acordo com a enumeração contida no PDF do desafio;

#### 1.0

Esta etapa projeto automatiza o processo de ETL (Extração, Transformação e Carga) das demonstrações contábeis da ANS, focado em isolar e consolidar as despesas assistenciais (Sinistros) das operadoras de saúde.


##### 🛠️ Fluxo de Execução

1. **Ingestão:** O crawler identifica e baixa os 3 últimos trimestres contábeis e a base cadastral (CADOP).

2. **Processamento (Streaming & Chunks):** Arquivos são lidos em pedaços de 150 mil linhas via `CsvProcessor`. Os dados filtrados são gravados em tempo real em um arquivo único através de um `output_stream`, evitando gargalos de memória e disco.

3. **Saneamento CADOP:** A base de operadoras é limpa, removendo duplicatas de CNPJ e priorizando o registro mais recente para garantir a fidelidade da Razão Social atualizada.

4. **Consolidação:** O pipeline une os dados financeiros ao cadastro. Para registros contábeis cujos IDs não constam no CADOP, o sistema preenche o CNPJ e a Razão Social como **"NÃO ENCONTRADO"**, preservando a integridade da massa de dados para auditoria.

##### ⚖️ Decisões Técnicas (Trade-offs)

* **Motivação do Grupo 41 (Sinistros):** O foco exclusivo no prefixo "41" deve-se ao fato de representarem os **Eventos Indenizáveis (Sinistros)**. Diferente de despesas administrativas, o Grupo 41 revela o custo real da assistência à saúde, sendo o principal indicador de solvência e eficiência de uma operadora.

<img width="993" height="323" alt="image" src="https://github.com/user-attachments/assets/6ed6ff86-bef1-4dff-93d9-12cd34763caf" />


* **Performance e Escalabilidade (Chunking):** O uso de `chunksize` no Pandas permite o processamento de arquivos com milhões de registros de forma incremental, evitando a carga total dos dados em memória. Essa abordagem garante que o pipeline opere dentro dos limites de RAM da máquina, tornando-o escalável para grandes volumes de dados.
    
* **Consolidação com base no Trimestre:** Os dados são agrupados por CNPJ, Ano e Trimestre, unificando múltiplos registros pertencentes à mesma empresa em um único resultado consolidado. Isso fornece uma visão mais clara e consistente da saúde financeira da entidade jurídica ao longo dos trimestres.

* **Reutilização de Conexão para Download:** A sessão HTTP é mantida ativa entre os downloads, evitando a necessidade de reestabelecer uma nova conexão a cada arquivo. Essa estratégia reduz overhead de handshake, melhora a eficiência da transferência e resulta em downloads mais rápidos.

* **Extensibilidade para Novos Formatos de Arquivo:** O sistema foi projetado de forma extensível por meio do padrão `Processor Registry`. Novos formatos podem ser suportados simplesmente criando uma classe que herde de `BaseProcessor` e registrando-a no pipeline. Um exemplo prático dessa extensão é o `TxtProcessor`.

* **Processamento em Streaming:** Durante o processamento e validação das linhas, o arquivo de saída é mantido aberto e escrito de forma contínua. Isso evita ciclos repetidos de abertura e fechamento de arquivo, reduz I/O desnecessário e melhora significativamente o desempenho do pipeline.
