# MVP de Engenharia de Dados
Projeto da disciplina Engenharia de Dados da especialização online Pós-Graduação em Ciência de Dados e Analytics, do Departamento de Informática da PUC-Rio.

Nome: Gabriela Padilha Carestiato Daniel

Matrícula: 4052025000831

## Objetivo
O objetivo deste trabalho é desenvolver um MVP de Engenharia de Dados para analisar o impacto das lesões em jogadores de oito clubes da Premier League, no período de 2019 a 2023, utilizando um ambiente de computação em nuvem, especificamente o Databricks. O problema a ser abordado consiste em compreender como as lesões afetam o desempenho e a disponibilidade dos atletas ao longo das temporadas, bem como identificar padrões que possam auxiliar os clubes na gestão de seus elencos e no planejamento físico dos jogadores. Para essa análise, são consideradas variáveis como: nome do jogador lesionado, clube ao qual pertencia no momento da lesão, posição em campo, idade do jogador no momento da lesão, temporada da ocorrência, rating FIFA correspondente à temporada, descrição da lesão sofrida, data da lesão e data de retorno às atividades.

Para isso, será construída uma pipeline completa de dados, contemplando as etapas de:

* Ingestão e coleta dos dados brutos
* Modelagem dos dados de forma adequada para análise
* Transformações e tratamento das informações
* Carga dos dados em um ambiente analítico em nuvem

Ao final da pipeline, será realizada uma análise exploratória e analítica com o objetivo de responder às perguntas de negócio.

### Perguntas a serem respondidas:
1. Qual é a frequência de lesões por temporada e por time?
2. Quais posições em campo apresentam maior incidência de lesões?
3. Existe relação entre a idade do jogador e a duração da lesão?
4. Jogadores com maior rating FIFA sofrem mais ou menos lesões?
5. Qual é o tempo médio de recuperação dos jogadores lesionados?
6. Quais times apresentam maior número de jogadores lesionados?
7. Existe relação entre o tipo de lesão e o tempo de retorno?

## Coleta
Os dados utilizados neste projeto foram obtidos a partir do dataset “Player Injuries and Team Performance Dataset”, disponível na plataforma [Kaggle](https://www.kaggle.com/datasets/amritbiswas007/player-injuries-and-team-performance-dataset). O dataset é fornecido em formato CSV e contém informações sobre lesões de jogadores da Premier League entre os anos de 2019 e 2023. O arquivo CSV foi armazenado inicialmente no repositório GitHub, na pasta `dataset`, e posteriormente carregado para o ambiente Databricks, onde foi persistido na camada Bronze do Data Lake, garantindo reprodutibilidade e rastreabilidade dos dados brutos.

## Modelagem
O projeto adota uma arquitetura de dados baseada nas camadas Bronze, Silver e Gold, seguindo boas práticas de Engenharia de Dados em ambientes de Data Lake. Além disso, ele utiliza um modelo de dados analítico flat, típico de arquiteturas de Data Lake, onde cada tabela representa um conceito de negócio específico, com granularidade adequada ao tipo de análise realizada. Não foi adotado um Esquema Estrela tradicional, pois o objetivo do projeto é responder perguntas analíticas pontuais a partir de dados agregados, priorizando simplicidade, performance e flexibilidade analítica. A camada Silver representa o nível de maior granularidade (evento de lesão por jogador), enquanto a camada Gold contém visões agregadas derivadas desse nível base.

### Qualidade da Modelagem

#### Camada Bronze - `bronze.premierleague`
A camada Bronze armazena os dados brutos ingeridos diretamente do dataset original em formato CSV, mantendo a estrutura próxima à fonte e preservando todas as colunas disponíveis. Nessa etapa, foram realizadas apenas padronizações básicas de nomenclatura das colunas, sem exclusão de registros ou transformações semânticas, garantindo a rastreabilidade e a possibilidade de reprocessamento dos dados.

**Papel da Bronze**
* Armazenar dados brutos
* Manter fidelidade à fonte
* Servir como histórico e reprocessamento
* Persistência em formato Delta Table no Databricks

#### Camada Silver - `silver.premierleague`
A camada Silver contém os dados tratados e modelados para análise. Nessa etapa, foram selecionadas apenas as colunas relevantes para o problema de negócio, além da aplicação de transformações e padronizações que garantem maior qualidade e consistência dos dados.

**Papel da Silver**
* Disponibilizar dados tratados, padronizados e confiáveis para análises e agregações
 
**Principais transformações**
* Redução de 42 colunas para 8 colunas analíticas
* Conversão de datas inconsistentes
* Remoção de registros com retorno "present"
* Correção de erros de digitação em datas (0202 → 2020)
* Criação da métrica: `duração_dias = dt_retorno - dt_lesão`
* Padronização textual: `posição` e `lesão`

Essa camada serve como base para as análises exploratórias e para a resposta às perguntas de negócio propostas.

#### Camada Gold - `gold.premierleague`
A camada Gold é responsável por armazenar dados agregados e analíticos, derivados da camada Silver, com foco direto na resposta às perguntas de negócio definidas no objetivo do projeto. Cada tabela dessa camada representa uma métrica consolidada, pronta para consumo analítico e visualizações.

**Tabelas criadas**
| Tabela           | Descrição                                                     | Pergunta de negócio                                          |
| ---------------- | ------------------------------------------------------------- | ------------------------------------------------------------ |
| `gold.pergunta1` | Frequência de lesões por temporada e por time                 | Qual a frequência de lesões por temporada e por time?        |
| `gold.pergunta2` | Incidência de lesões por posição                              | Quais posições apresentam maior incidência de lesões?        |
| `gold.pergunta3` | Duração média da lesão por faixa etária                       | Existe relação entre idade e duração da lesão?               |
| `gold.pergunta4` | Frequência de lesões por faixa de rating FIFA                 | Jogadores com maior rating FIFA sofrem mais ou menos lesões? |
| `gold.pergunta5` | Tempo médio geral de recuperação                              | Qual o tempo médio de recuperação dos jogadores lesionados?  |
| `gold.pergunta6` | Número de jogadores lesionados por time                       | Quais times apresentam maior número de jogadores lesionados? |
| `gold.pergunta7` | Relação entre tipo de lesão e tempo médio de retorno (Top 10) | Existe relação entre o tipo de lesão e o tempo de retorno?   |

Todas as tabelas da camada Gold foram persistidas no Databricks em formato Delta Table, garantindo consistência, reprodutibilidade e facilidade de consumo para análises posteriores.

#### Data Lineage
A linhagem dos dados segue o fluxo abaixo:

1. **Fonte dos dados**

Dataset público “Player Injuries and Team Performance Dataset”, disponibilizado na plataforma Kaggle, em formato CSV.

2. **Camada Bronze**

Ingestão direta dos arquivos CSV no Databricks, preservando o conteúdo original, com mínimas padronizações de nomenclatura.

3. **Camada Silver**

Aplicação de regras de limpeza, padronização, correção de inconsistências e criação de métricas derivadas, resultando em um dataset confiável para análise.

4. **Camada Gold**

Criação de tabelas agregadas e analíticas, orientadas às perguntas de negócio, prontas para consumo em dashboards e análises exploratórias.

### Catálogo de Dados (Camada de Bronze)
<img width="1142" height="537" alt="image" src="https://github.com/user-attachments/assets/50d6aee8-ded1-4dbb-b58d-bd325196f38c" />
<img width="1142" height="517" alt="image" src="https://github.com/user-attachments/assets/c03bb149-abb3-4f76-9874-03f21729b273" />
<img width="1143" height="50" alt="image" src="https://github.com/user-attachments/assets/072369cf-53bf-4a15-a642-2211c7360550" />

### Catálogo de Dados (Camada de Prata)
<img width="813" height="367" alt="image" src="https://github.com/user-attachments/assets/e814a297-f1a5-4216-9972-3c8649c2f70a" />


