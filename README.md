# Projeto de Clusterização de Dados

Este projeto é uma solução completa para realizar a clusterização de dados, desde a ingestão e processamento inicial até o treinamento e avaliação de modelos de machine learning. O sistema é dividido em dois componentes principais: um módulo de ingestão de dados e um pipeline de treinamento de modelos de clusterização.

## ✨ Funcionalidades

- **Módulo de Ingestão de Dados:** Coleta e prepara dados brutos de forma automatizada.
- **Processamento e Limpeza:** Pipelines para validar, limpar e transformar os dados.
- **Engenharia de Features:** Criação e seleção de features para otimizar o desempenho dos modelos.
- **Treinamento de Modelos:** Suporte para múltiplos algoritmos de clusterização, como K-Means e Gaussian Mixture Models (GMM).
- **Estrutura Modular:** Código organizado em componentes reutilizáveis, facilitando a manutenção e a expansão.

## 📂 Estrutura do Projeto

O projeto está organizado nos seguintes diretórios principais:

- **`/data_ingestion`**: Módulo responsável pela coleta e armazenamento inicial dos dados. Contém sua própria lógica, CLI e configurações.
- **`/src`**: Contém o código principal da aplicação, incluindo os pipelines de processamento, treinamento de modelos e utilitários.
- **`/notebooks`**: Jupyter Notebooks para análise exploratória, testes e prototipagem.
- **`/main.py`**: Ponto de entrada principal para orquestrar os pipelines do projeto.
- **`/requirements.txt`**: Lista de dependências Python do projeto.

## 🚀 Como Começar

Siga as instruções abaixo para configurar e executar o projeto em seu ambiente local.

### Pré-requisitos

- Python 3.9 ou superior
- Git

### Instalação

1.  Clone o repositório para sua máquina local:
    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd <NOME_DO_PROJETO>
    ```

2.  Crie um ambiente virtual e ative-o:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # No Windows, use: .venv\Scripts\activate
    ```

3.  Instale as dependências necessárias:
    ```bash
    pip install -r requirements.txt
    ```

## 🛠️ Uso

A execução do projeto é dividida em duas etapas principais: ingestão de dados e treinamento do pipeline.

### 1. Ingestão de Dados

O módulo `data_ingestion` é responsável por baixar e processar os dados brutos. Ele possui uma interface de linha de comando (CLI) própria para iniciar o processo. Para mais detalhes, consulte o `README.md` dentro do diretório `data_ingestion`.

Para executar a ingestão, navegue até o diretório e execute o script principal:
```bash
python data_ingestion/main.py <COMANDOS_DA_CLI>
```

### 2. Pipeline de Treinamento

Após a conclusão da etapa de ingestão, os dados estarão prontos para serem processados e utilizados no treinamento dos modelos. O script `main.py` na raiz do projeto orquestra todas as etapas do pipeline principal.

Para executar o pipeline completo (processamento, seleção de features e treinamento), execute:
```bash
python main.py
```

## ⚙️ Configuração

As configurações do projeto, como caminhos de arquivos, parâmetros de modelos e configurações de ambiente, podem ser encontradas e modificadas nos seguintes locais:

- **Ingestão de Dados:** `data_ingestion/config/`
- **Pipeline Principal:** `src/config/`
