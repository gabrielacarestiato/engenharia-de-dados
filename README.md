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

### Discussão dos Resultados

**1. Frequência de lesões por temporada e time**

* Tabela de Resultados
  
A tabela abaixo apresenta a quantidade de lesões registradas por time em cada temporada analisada, entre 2019/20 e 2023/24.

| Temporada | Time        | Nº de Lesões |
| --------- | ----------- | ------------ |
| 2019/20   | Arsenal     | 25           |
| 2019/20   | Aston Villa | 11           |
| 2019/20   | Burnley     | 22           |
| 2019/20   | Everton     | 36           |
| 2019/20   | Man United  | 24           |
| 2019/20   | Newcastle   | 31           |
| 2019/20   | Tottenham   | 21           |
| 2020/21   | Arsenal     | 25           |
| 2020/21   | Aston Villa | 8            |
| 2020/21   | Burnley     | 26           |
| 2020/21   | Everton     | 20           |
| 2020/21   | Man United  | 20           |
| 2020/21   | Newcastle   | 25           |
| 2020/21   | Tottenham   | 17           |
| 2021/22   | Arsenal     | 10           |
| 2021/22   | Aston Villa | 9            |
| 2021/22   | Brentford   | 6            |
| 2021/22   | Burnley     | 13           |
| 2021/22   | Everton     | 17           |
| 2021/22   | Man United  | 36           |
| 2021/22   | Newcastle   | 15           |
| 2021/22   | Tottenham   | 23           |
| 2022/23   | Arsenal     | 12           |
| 2022/23   | Aston Villa | 10           |
| 2022/23   | Brentford   | 7            |
| 2022/23   | Everton     | 16           |
| 2022/23   | Man United  | 21           |
| 2022/23   | Newcastle   | 15           |
| 2022/23   | Tottenham   | 15           |
| 2023/24   | Arsenal     | 12           |
| 2023/24   | Aston Villa | 21           |
| 2023/24   | Brentford   | 21           |
| 2023/24   | Burnley     | 9            |
| 2023/24   | Everton     | 14           |
| 2023/24   | Newcastle   | 34           |
| 2023/24   | Tottenham   | 6            |

* Análise do Gráfico
  
O gráfico de linhas ilustra a evolução do número de lesões por time ao longo das temporadas, permitindo identificar padrões temporais e diferenças entre os clubes.

<img width="1222" height="375" alt="image" src="https://github.com/user-attachments/assets/f20324eb-7ac7-44a5-b562-18442f696bbe" />

**Pontos relevantes observados**

* Alta variabilidade entre clubes: Times como Everton, Newcastle e Manchester United apresentam picos elevados de lesões em determinadas temporadas, enquanto outros, como Aston Villa e Brentford, mantêm números relativamente mais baixos.
* Picos específicos por temporada:
- O Everton apresenta um pico significativo na temporada 2019/20.
- O Manchester United registra um aumento expressivo em 2021/22, destacando-se como o time com maior número de lesões nessa temporada.
- O Newcastle apresenta crescimento acentuado em 2023/24, atingindo o maior valor observado no período.
* Tendências de queda: Alguns clubes, como Arsenal e Tottenham, apresentam redução no número de lesões ao longo das temporadas, sugerindo possíveis melhorias em gestão física, elenco ou estratégias de rotação.
* Impacto de contexto competitivo: As oscilações ao longo do tempo podem estar relacionadas a fatores como calendário mais intenso, mudanças no elenco, estilo de jogo ou participação em competições paralelas.

**2. Incidência de lesões por posição**

* Tabela de Resultados
  
A tabela abaixo apresenta o número total de lesões registradas por posição dos jogadores ao longo do período analisado.

| Posição              | Nº de Lesões |
|----------------------|--------------|
| Center Back          | 124          |
| Central Midfielder   | 113          |
| Center Forward       | 98           |
| Left Winger          | 60           |
| Left Back            | 57           |
| Defensive Midfielder | 55           |
| Right Back           | 53           |
| Right Winger         | 42           |
| Attacking Midfielder | 24           |
| Goalkeeper           | 20           |
| Left Midfielder      | 4            |
| Right Midfielder     | 3            |

* Análise do Gráfico
  
O gráfico de barras apresenta a distribuição do número de lesões por posição, permitindo identificar quais funções em campo estão mais expostas a ocorrências de lesão.

<img width="1310" height="375" alt="image" src="https://github.com/user-attachments/assets/35946dd9-342b-4b57-9958-55f64ddf67c0" />

**Pontos relevantes observados**

* Maior incidência em posições defensivas centrais: As posições de Center Back e Central Midfielder apresentam o maior número de lesões, indicando maior exposição física devido a contato frequente, disputas aéreas e alta intensidade defensiva.

* Alta carga em posições ofensivas-chave: O Center Forward também apresenta um número elevado de lesões, possivelmente associado a acelerações frequentes, mudanças bruscas de direção e contato físico constante com defensores.

* Distribuição intermediária nas laterais: Laterais e pontas (Left/Right Back e Left/Right Winger) apresentam valores intermediários, refletindo a exigência física de percorrer grandes distâncias ao longo das partidas.

* Baixa incidência em posições específicas: As posições de Goalkeeper, Left Midfielder e Right Midfielder apresentam os menores números de lesões, sugerindo menor exposição a choques frequentes ou menor volume de ações de alta intensidade.

* Implicações para gestão esportiva: Os resultados indicam a necessidade de maior atenção na prevenção de lesões em posições centrais do campo, com foco em estratégias de rotação, monitoramento de carga física e programas de recuperação adequados.

**3. Média de duração das lesões por faixa etária**

* Tabela de Resultados

A tabela abaixo apresenta a duração média das lesões (em dias) de acordo com a faixa etária dos jogadores.

| Faixa Etária | Duração Média (dias) |
|---------------|---------------------|
| 18–24         | 51,23               |
| 25–28         | 41,48               |
| 29–32         | 46,27               |
| 33–40         | 58,20               |

* Análise do Gráfico

O gráfico de barras ilustra a relação entre faixa etária e a duração média das lesões, permitindo identificar diferenças no tempo de recuperação entre grupos etários.

<img width="1310" height="375" alt="image" src="https://github.com/user-attachments/assets/54b9dced-fc7e-407b-ab1b-04b7cee23df2" />

**Pontos relevantes observados**

* Jogadores mais jovens (18–24): Apresentam uma duração média relativamente elevada de lesões, possivelmente associada a processos de adaptação física, intensidade elevada de jogo e menor histórico de controle de carga.

* Faixa etária de 25–28 anos: Registra o menor tempo médio de recuperação, sugerindo um equilíbrio entre maturidade física, desempenho atlético e capacidade de recuperação.

* Aumento gradual a partir dos 29 anos: A duração média das lesões volta a crescer na faixa de 29–32 anos, indicando possíveis efeitos cumulativos de desgaste físico ao longo da carreira.

* Maior impacto em jogadores veteranos (33–40): Esta faixa apresenta a maior duração média de lesões, refletindo processos naturais de envelhecimento, maior tempo de recuperação muscular e maior risco de lesões recorrentes.

* Implicações para gestão esportiva: Os resultados reforçam a importância de estratégias diferenciadas de prevenção e recuperação de lesões por faixa etária, especialmente para atletas mais experientes.

**4. Frequência de lesões por faixa de rating FIFA**

* Tabela de Resultados
  
A tabela abaixo apresenta a quantidade de lesões registradas de acordo com a faixa de rating FIFA dos jogadores.

| Faixa de Rating FIFA | Nº de Lesões |
|----------------------|--------------|
| 76–80                | 310          |
| 81–85                | 167          |
| ≤ 75                 | 148          |
| > 85                 | 28           |

* Análise do Gráfico
  
O gráfico de rosca representa o percentual de lesões por faixa de rating FIFA, permitindo avaliar a distribuição relativa das lesões entre diferentes níveis de desempenho dos jogadores.

<img width="1241" height="375" alt="image" src="https://github.com/user-attachments/assets/3eecca87-b2d1-4094-9b30-95baee54d031" />

**Pontos relevantes observados**

* Maior concentração entre ratings intermediários (76–80): Esta faixa concentra quase metade das lesões observadas, indicando que jogadores de nível intermediário-alto são os mais expostos a lesões.

* Redução progressiva em ratings mais altos: Jogadores com rating entre 81–85 apresentam menos lesões, e aqueles com rating superior a 85 representam a menor parcela do total.

* Possível efeito de gestão física: Atletas de maior rating tendem a ter melhor acompanhamento médico, controle de carga e estratégias de preservação física, o que pode contribuir para a menor incidência de lesões.

* Impacto do volume de jogadores: A maior frequência nas faixas intermediárias também pode estar relacionada ao maior número de jogadores nessas categorias em comparação com atletas de elite (>85).

* Implicações analíticas: Os resultados sugerem que maior rating FIFA não está associado a maior incidência de lesões, contrariando a hipótese de que jogadores mais exigidos fisicamente se lesionam com m

**5. Tempo médio geral de recuperação**

* Tabela de Resultados
  
O tempo médio geral de recuperação dos jogadores lesionados, considerando todo o período analisado, é apresentado abaixo.

| Métrica                         | Valor (dias) |
|--------------------------------|--------------|
| Tempo médio de recuperação     | 46,42        |

* Análise do Resultado
  
O valor médio de aproximadamente **46 dias** indica que, de forma geral, as lesões analisadas resultam em afastamentos superiores a um mês e meio, o que pode gerar impactos relevantes no desempenho esportivo das equipes ao longo das temporadas.

**Pontos relevantes observados**

* Impacto competitivo: Um tempo médio de recuperação elevado implica maior necessidade de rotação de elenco, utilização de reservas e possíveis perdas técnicas durante a temporada.

* Variabilidade não capturada pela média: Embora o valor médio seja de 46 dias, existem lesões de curta duração e outras significativamente mais longas, o que reforça a importância de análises segmentadas por tipo de lesão, posição e idade.

* Relevância para gestão esportiva:Esse indicador é fundamental para decisões estratégicas relacionadas à prevenção de lesões, planejamento físico e investimento em infraestrutura médica.

* Base para análises complementares: O tempo médio geral serve como referência para comparações com recortes mais específicos, como faixas etárias, tipo de lesão e rating FIFA, explorados nas demais questões do projeto.

