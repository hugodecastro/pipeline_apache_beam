## Description:

Pipeline de dados utilizando o Apache Beam para captação e tratamento de dados em um arquivo CSV.

## Tools
- Python 3.9
- Apache Beam

## Setup
### Localhost
- git clone https://github.com/hugodecastro/pipeline_apache_beam.git
- pip install -r requirements.txt
### Run
- Rode os seguintes comandos na ordem especificada:
- **AVISO**: Antes de rodar os comandos, certifique-se que os caminhos estão configurados corretamente

```
python beams/estados/beam_estados.py
python beams/covid/beam_covid.py
python beams/result.py
python beams/to_json.py

```
## Results
- Se todos os arquivos forem executados corretamente, os resultados gerados serão um arquivo CSV e um JSON dentro da pasta resources/final
- O resultado final esperado pode ser visto [aqui](https://github.com/hugodecastro/pipeline_apache_beam/blob/main/resources/final/result.PNG)


## Authors
- [@hugodecastro](https://github.com/hugodecastro)
