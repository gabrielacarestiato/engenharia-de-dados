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

------------------------------------

## Coleta
Os dados utilizados neste projeto foram obtidos a partir do dataset “Player Injuries and Team Performance Dataset”, disponível na plataforma [Kaggle](https://www.kaggle.com/datasets/amritbiswas007/player-injuries-and-team-performance-dataset). O dataset é fornecido em formato CSV e contém informações sobre lesões de jogadores da Premier League entre os anos de 2019 e 2023. O arquivo CSV foi armazenado inicialmente no repositório GitHub, na pasta `dataset`, e posteriormente carregado para o ambiente Databricks, onde foi persistido na camada Bronze do Data Lake, garantindo reprodutibilidade e rastreabilidade dos dados brutos.

------------------------------------

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
* Remoção de registros com retorno `"present"`
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

### Data Lineage
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
<img width="905" height="525" alt="image" src="https://github.com/user-attachments/assets/8e255463-c287-4f68-adb0-42eaf728fe2c" />

------------------------------------

## Carga
A carga dos dados foi realizada no ambiente Databricks por meio de pipelines de ETL implementadas em PySpark, organizadas em notebooks distintos para cada camada do Data Lake (Bronze, Silver e Gold). Essa separação garante clareza no fluxo de dados, reprodutibilidade e facilidade de manutenção da solução.

O pipeline inicia com a ingestão do dataset original em formato CSV, armazenado no repositório GitHub do projeto, que é carregado e persistido na camada Bronze em formato Delta Table. Em seguida, os dados são transformados e tratados na camada Silver, onde são aplicadas regras de limpeza, padronização e criação de métricas derivadas.

Por fim, a carga na camada Gold consiste na geração de tabelas analíticas agregadas, diretamente alinhadas às perguntas de negócio propostas. Cada tabela Gold é persistida como Delta Table no Databricks, garantindo consistência, versionamento e confiabilidade no consumo analítico.

Todas as etapas de carga utilizam operações nativas do Spark `(write.format("delta").saveAsTable)`, assegurando escalabilidade e correta persistência dos dados no ambiente de nuvem.

------------------------------------

## Análise
Esta etapa tem como objetivo avaliar a qualidade dos dados utilizados e analisar os resultados obtidos a partir das tabelas analíticas da camada Gold, verificando se as perguntas de negócio propostas foram corretamente respondidas e discutindo os principais padrões observados.

### Análise da Qualidade dos Dados
A qualidade dos dados foi avaliada principalmente na camada Silver, que representa o nível mais granular do modelo (evento de lesão por jogador). Durante essa etapa, foram realizadas verificações de consistência, completude e coerência semântica dos dados.

**Principais pontos observados**
* Valores ausentes: foram identificados valores inconsistentes nas colunas de data, especialmente na data de retorno, contendo o valor textual `"present"`. Esses registros foram removidos, pois inviabilizavam o cálculo da duração da lesão.

* Padronização de datas: o dataset apresentava múltiplos formatos de data, além de erros de digitação no ano (ex.: `0202`). Essas inconsistências foram tratadas e normalizadas para o tipo `date`.

* Coerência temporal: foram identificados casos em que a data de retorno era anterior à data da lesão. Para esses casos, foi aplicada uma correção lógica, somando um ano à data de retorno, garantindo coerência temporal.

* Domínios categóricos: as colunas categóricas (posição e lesão) foram padronizadas (remoção de espaços extras e normalização para minúsculas), reduzindo duplicidades semânticas.

* Faixas esperadas: os valores numéricos ficaram dentro dos limites esperados, como idade entre 18 e 39 anos, rating FIFA entre 66 e 90 e duração de lesão positiva.

Após essas etapas, o dataset apresentou boa qualidade para análise, com dados consistentes, padronizados e adequados para agregações analíticas.

### Análise da Solução do Problema
As tabelas da camada Gold permitiram responder corretamente todas as perguntas de negócio propostas no objetivo do projeto:

**1. Frequência de lesões por temporada e time**
A análise mostra variações claras na quantidade de lesões ao longo das temporadas e entre os clubes, indicando possíveis diferenças na intensidade de jogos, profundidade do elenco ou políticas de preparação física.

**2. Incidência de lesões por posição**
Observa-se que determinadas posições apresentam maior incidência de lesões, especialmente posições com maior exigência física, como defensores e meio-campistas, o que está alinhado com o conhecimento do domínio do futebol.

**3. Relação entre idade e duração da lesão**
A segmentação por faixa etária indica que jogadores mais velhos tendem a apresentar maior tempo médio de recuperação, sugerindo que a idade pode influenciar diretamente na duração das lesões.

**4. Lesões por faixa de rating FIFA**
A análise por faixa de rating mostra que jogadores com diferentes níveis de performance sofrem lesões em frequências distintas, permitindo avaliar se atletas mais valorizados estão mais expostos ou preservados.

**5. Tempo médio de recuperação**
O cálculo do tempo médio geral de recuperação fornece uma visão consolidada do impacto das lesões na disponibilidade dos jogadores.

**6. Times com maior número de jogadores lesionados**
Alguns clubes concentram um maior número de jogadores lesionados, o que pode indicar diferenças em calendário, elenco ou estratégias de gestão física.

**7. Relação entre tipo de lesão e tempo de retorno**
A análise dos tipos de lesão mostra que certas lesões apresentam tempos médios de recuperação significativamente maiores, reforçando a importância de distinguir o impacto de cada tipo de lesão.
