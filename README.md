# Desafio - Intuitive Care

## Tecnologias
> Python 
> POSTGRESql 
> VueJs

## Importante !

O README está dividido em sessões de acordo com o PDF enviado por email, justificativas de determinadas
decisões podem ser encontradas nas sessões à qual pertence a etapa;

#### Estrutura:


### 🏗️ Arquitetura e Decisões Técnicas (Etapa 1)

O pipeline foi construído com foco em **escalabilidade** e **baixo consumo de recursos**, utilizando os seguintes padrões:

#### 1. Processamento Extensível (Template Method Pattern)

Utilizei a classe abstrata `BaseProcessor` para padronizar o comportamento de processamento. Isso permite que o sistema suporte novos formatos (CSV, TXT, XLSX) apenas estendendo a classe, mantendo a lógica de filtragem e salvamento centralizada.

#### 2. Trade-off: Memória vs. Performance (Incremental Saving)

* **Decisão:** Em vez de carregar todos os DataFrames na memória para consolidá-los ao final, implementei o método `_save_incremental` utilizando o modo **`append ('a')`** do Pandas.
* **Pró:** O pipeline processa arquivos de centenas de megabytes com um consumo de memória RAM constante e baixo, pois escreve os dados filtrados no disco assim que terminam de ser processados.
* **Contra:** Há um pequeno *overhead* de I/O por abrir/fechar o arquivo repetidamente, mas que é compensado pela segurança de não sofrer um crash por ocupar toda a RAM*.

#### 3. Filtragem Antecipada (Early Filtering)

* **Estratégia:** O filtro pelo prefixo **"41"** (Sinistros/Eventos) é aplicado imediatamente após a leitura de cada arquivo.
* **Justificativa:** Reduzimos a massa de dados em mais de 80% logo na origem, garantindo que as etapas de Join e Transformação trabalhem apenas com o "ouro" (dados assistenciais), otimizando todo o fluxo subsequente.

---

### 🛠️ Especificações do Pipeline

* **Resiliência:** Validação de extensões e tratamento de colunas ausentes (`CD_CONTA_CONTABIL`).
* **Encoding:** Uso de `utf-8-sig` para garantir compatibilidade com Excel e caracteres especiais brasileiros.
* **Consistência:** Garantia de cabeçalho único no arquivo consolidado, mesmo em modo append.
---

