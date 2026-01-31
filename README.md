# Desafio - Intuitive Care

## Tecnologias
> Python 
> POSTGRESql 
> VueJs

## Importante !

O README está dividido em sessões de acordo com o PDF enviado por email, justificativas de determinadas
decisões podem ser encontradas nas sessões à qual pertence a etapa;

## Estrutura do monorepo:

```

```

````
```
```
```
```
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
* **Performance (Chunking):** O uso de `chunksize` no Pandas garante que o pipeline processe milhões de registros sem ultrapassar o limite de RAM da máquina, tornando o sistema escalável para volumes massivos de dados.
* **Consolidação por CNPJ:** Optamos por agrupar os dados por CNPJ/Ano/Trimestre. Isso unifica diferentes registros de uma mesma empresa, entregando uma visão consolidada da saúde financeira da entidade jurídica.

