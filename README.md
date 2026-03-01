Camada Bronze:
A camada Bronze é responsável pela ingestão dos dados brutos.
Este código implementa o pipeline da camada Bronze dentro de uma arquitetura de dados no modelo Medallion. Sua principal função é realizar a ingestão de dados brutos, carregando um arquivo JSON contendo informações do Jira e convertendo-o para um formato estruturado e eficiente para armazenamento.
O processo começa com a leitura do arquivo JSON a partir de um caminho definido localmente. O carregamento é feito com tratamento básico de erros para evitar falhas inesperadas caso o arquivo não seja encontrado ou esteja corrompido. Após a leitura, os dados são convertidos para um DataFrame do pandas utilizando a função json_normalize, que permite estruturar dados aninhados e transformá-los em formato tabular.
Importante destacar que nesta etapa não há qualquer transformação de negócio, cálculo, filtragem ou enriquecimento dos dados. O objetivo da camada Bronze é apenas organizar os dados brutos mantendo sua integridade e fidelidade à origem.
Em seguida, o DataFrame é salvo no formato Parquet dentro da pasta Bronze. O formato Parquet é utilizado por ser colunar e otimizado para desempenho e compressão, facilitando etapas futuras de processamento.

Camada Silver
Responsável por transformar e organizar os dados estruturados provenientes da camada Bronze.
O processo inicia com a leitura do arquivo jira_issues_raw.parquet, armazenado na camada Bronze. A partir desse dataset bruto estruturado, o código realiza tratamentos e normalizações para gerar conjuntos de dados mais organizados e preparados para análises futuras.
Primeiramente, é feito o tratamento da coluna assignee. Como essa coluna pode conter listas ou estruturas aninhadas, o código utiliza explode para expandir os registros e json_normalize para transformar os dados em formato tabular. Em seguida, são selecionadas apenas as colunas relevantes (id, name e email), removidas duplicidades e ordenados os registros, resultando no DataFrame df_dados_equipe, que representa a dimensão de equipe.
Depois, é criado o DataFrame df_SLA, contendo informações essenciais para cálculos futuros de SLA, como identificador da issue, tipo, status, prioridade, timestamps e identificador do responsável. Nessa etapa, também é extraído o assignee_id a partir da estrutura aninhada.
Na sequência, o código cria o DataFrame df_timestamps, que trata especificamente os campos de datas. A coluna timestamps é expandida, normalizada e convertida para o tipo datetime com padronização em UTC. Datas inválidas são tratadas automaticamente com errors='coerce', evitando falhas no processamento. Os registros são ordenados e duplicidades removidas para garantir qualidade e consistência dos dados.
Após os tratamentos, o script reorganiza colunas, remove informações já processadas e garante padronização e integridade dos conjuntos de dados finais.
Por fim, os três DataFrames resultantes — equipe, SLA e timestamps — são salvos em formato Parquet na pasta da camada Silver. Essa camada passa a conter dados limpos, normalizados e estruturados, prontos para regras de negócio, cálculos analíticos e agregações que serão realizadas na camada Gold.
Em resumo, este código representa a etapa de transformação e refinamento da arquitetura Medallion, convertendo dados estruturados da Bronze em datasets organizados, consistentes e preparados para análises avançadas na camada Gold.

Camada Gold: 
A camada Gold é responsável por aplicar as regras de negócio e gerar os dados analíticos finais do pipeline. 
O processo inicia com a leitura dos arquivos Parquet provenientes da camada Silver, que contêm os dados organizados de equipe, informações de SLA e timestamps das issues.
Em seguida, o código define a regra de SLA esperado com base na prioridade da issue (High, Medium ou Low). Para cada registro, é calculado o tempo de resolução em horas úteis por meio da função calcular_intervalo_uteis_em_horas, considerando apenas chamados que não estejam com status “Open” e que possuam datas válidas de criação e resolução.
Com base nesse cálculo, o script constrói uma tabela fato final contendo: identificação da issue, tipo, responsável, prioridade, datas, horas de resolução, SLA esperado e um indicador booleano informando se o SLA foi cumprido.
Além da tabela detalhada, a camada Gold também gera duas agregações analíticas:
    SLA médio por responsável (assignee)
    SLA médio por tipo de issue
Por fim, os resultados são gravados na pasta Gold,conforme detalhado após apresentação do cálculo de SLA 

Utiliza uma função para cálculo de sla
A função calcular_intervalo_uteis_em_horas é responsável por calcular a quantidade de horas úteis entre duas datas, considerando apenas dias de segunda a sexta-feira. 
Ela é utilizada no processo de cálculo de SLA da camada Gold, permitindo medir o tempo efetivo de resolução das issues em horas úteis.
A função aplica algumas regras de validação importantes: 
    Se as datas forem nulas (None ou NaT), ou se a data final for menor ou igual à data inicial, o retorno é 0. 
    Também realiza a padronização dos valores para o tipo datetime utilizando a biblioteca pandas, além de remover informações de timezone para evitar conflitos entre datas “offset-naive” e “offset-aware”.
O cálculo é feito percorrendo o intervalo dia a dia. 
    Para cada dia, a função verifica se é um dia útil (segunda a sexta). 
    Caso seja, soma a diferença de tempo (em horas) entre o horário atual e o limite do próximo dia ou da data final. 
    Sábados e domingos são automaticamente ignorados. 
    Ao final, o total de horas úteis é retornado com duas casas decimais.
    A implementação utiliza a API pública da biblioteca pandas para manipulação e validação de datas (pd.to_datetime, pd.isna) e a biblioteca padrão datetime do Python para operações com datas e intervalos de tempo (datetime, timedelta).

Saídas produzidas:
    Tabela Final – SLA por Chamado             --> arquivo: jira_solution_final_table_gold.parquet
    Relatório de SLA Médio por Analista        --> arquivo: jira_solution_report_assignee_gold.csv
    Relatório de SLA Médio por Tipo de Chamado --> arquivo: jira_solution_report_id_gold.csv
    
Instalações necessárias:
Para intalação devem ser executado o arquivo de requirements.txt
     pip install -r requirements.txt
