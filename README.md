# MVP de Engenharia de Dados
Projeto da disciplina Engenharia de Dados da especialização online Pós-Graduação em Ciência de Dados e Analytics, do Departamento de Informática da PUC-Rio.

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
