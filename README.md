# Desafio - Intuitive Care

## Tecnologias
> Python 
> POSTGRESql 
> VueJs

## Importante !

O README está dividido em sessões de acordo com o PDF enviado por email, justificativas de determinadas
decisões podem ser encontradas nas sessões à qual pertence a etapa;

#### Estrutura:




#### 1.0 TESTE DE INTEGRAÇÃO COM API PÚBLICA

Em main.py é definido a BASE_URL que será utilizada para gerenciar o scraping e a pasta onde os arquivos a serem baixados ficarão (nomeada rawFiles);

O crawler então vai até a api e navega pelo HTML buscando pelos links de download dos últimos trimestres;

A url é passada para o downloader que faz download do zip de acordo com os links passados e adiciona os downloads para o diretório informado. Isso acontece de maneira concorrente porque é criado um Pool;


