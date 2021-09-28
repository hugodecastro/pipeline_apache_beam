# -*- coding: utf-8 -*-
import apache_beam as beam

painel = beam.Pipeline()

class parse_csv(beam.DoFn):
  """
    Retorna 'uf;total_casos;total_obitos'
  """
  def process(self, record):
    uf = record[0]
    total_casos = record[1][0][0]
    total_obitos = record[1][1][0]
    yield f'{uf};{total_casos};{total_obitos}'

class write_data(beam.DoFn):
  """
    Escreve a informação gerada no arquivo 'processed.txt' já existente
  """
  def process(self, element):
      with open(r"src\resources\processed\processed.txt", "a+") as f:
        f.write(element)
        f.write('\n')


PainelCovidCasos = (
  painel
  | "Importar Dados Painel Casos" >> beam.io.ReadFromText(r"src\resources\initial\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines = 1)
  | "Separar por ; ..." >> beam.Map(lambda record: record.split(';'))
  | "Filtrando painel casos" >> beam.Filter(lambda record: record[0] != 'Brasil') # remove as linhas com 'Brasil'
  | "Map casos" >> beam.Map(lambda record: (record[1], int(record[11]))) # mapeia o resultado como ( uf: totalCasos )
  | "Soma casos" >> beam.CombinePerKey(sum) # agrupa por UF e soma o total de casos
)

PainelCovidObitos = (
  painel
  | "Importar Dados Painel Obtos" >> beam.io.ReadFromText(r"src\resources\initial\HIST_PAINEL_COVIDBR_28set2020.csv", skip_header_lines = 1)
  | "Separar por ;" >> beam.Map(lambda record: record.split(';'))
  | "Filtrando painel obitos" >> beam.Filter(lambda record: record[0] != 'Brasil')
  | "Map obitos" >> beam.Map(lambda record: (record[1], int(record[13]))) # mapeia o resultado como ( uf: totalObitos )
  | "Soma obitos" >> beam.CombinePerKey(sum) # agrupa por UF e soma o total de obitos
  
)

PainelCovid = (
    [PainelCovidCasos, PainelCovidObitos] # agrupa os resultados gerados em PainelCovidCasos e PainelCovidObitos como uma lista
    | "Group By" >> beam.CoGroupByKey() # agrupa por UF
    | "Parse to CSV" >> beam.ParDo(parse_csv()) # formatação para salvar resultado
    | "Escrevendo dados" >> beam.ParDo(write_data())
)

painel.run()