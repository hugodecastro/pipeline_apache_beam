# -*- coding: utf-8 -*-
import apache_beam as beam
import util

estados = beam.Pipeline()

class filtro_estados_csv(beam.DoFn):
  """
    Retorna o registro filtrado contendo apenas: [região, estado, uf, governador]
  """
  def process(self, record):
    yield f'{record[0]};{record[1]};{record[2]};{record[3]}'
      
class parse_region(beam.DoFn):
  """
    Retorna os registros com suas respectivas regiões inseridas
  """
  def process(self, record):
    estado = record[0]
    for key, value in util.regions.items():
      if estado in value:
        record.insert(0, key)
        yield record

class parse_uf(beam.DoFn):

  def process(self, record):
    """
      Retorna os registros com suas respectivas UFs inseridas
    """
    estado = record[1]
    for key, value in util.ufs.items():
      if estado == value:
        record.insert(0, key)
        yield record # reagrupa o resultado como lista e retorna

EstadosIBGE = (
  estados
  | "Importar Dados" >> beam.io.ReadFromText(r"src\resources\initial\EstadosIBGE.csv", skip_header_lines=1)
  | "Separar dados por ';'" >> beam.Map(lambda record: record.split(';'))
  | "Filtrar estados" >> beam.Map(lambda record: [record[0], record[3]])
  | "Mapeando regiões" >> beam.ParDo(parse_region())
  | "Mapeando UF" >> beam.ParDo(parse_uf())
  | "Filtrando resultado" >> beam.ParDo(filtro_estados_csv())
  | "Saida" >> beam.io.WriteToText(file_path_prefix=r"src\resources\processed\processed",
                                            file_name_suffix='.txt', 
                                            shard_name_template='')
)

estados.run()