# -*- coding: utf-8 -*-
import apache_beam as beam

estados = beam.Pipeline()

class filtro_estados_csv(beam.DoFn):
  """
    Retorna o registro filtrado contendo apenas: [região;estado;uf;governador]
  """
  def process(self, record):
    yield f'{record[0]};{record[1]};{record[2]};{record[3]}'
      
class parse_region(beam.DoFn):
  # extraído de https://www.infoescola.com/geografia/regioes-e-estados-brasileiros/
  regions = {
      "Norte": "Amazonas, Roraima, Amapá, Pará, Tocantins, Rondônia, Acre",
      "Nordeste": "Maranhão, Piauí, Ceará, Rio Grande do Norte, Pernambuco, Paraíba, Sergipe, Alagoas, Bahia",
      "Centro-Oeste": "Mato Grosso, Mato Grosso do Sul, Goiás, Distrito Federal",
      "Sudeste": "São Paulo, Rio de Janeiro, Espírito Santo, Minas Gerais",
      "Sul": "Paraná, Rio Grande do Sul, Santa Catarina"
  }

  def process(self, record):
    """
      Retorna os registros com suas respectivas regiões inseridas
    """
    estado = record[0]
    for key, value in self.regions.items():
      if estado in value:
        record.insert(0, key)
        yield record

class parse_uf(beam.DoFn):
  ufs = {
      "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas", "BA": "Bahia", "CE": "Ceará",
      "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão", "MT": "Mato Grosso",
      "MS": "Mato Grosso do Sul", "MG": "Minas Gerais", "PA": "Pará", "PB": "Paraíba", "PR": "Paraná",
      "PE": "Pernambuco", "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
      "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina",
      "SP": "São Paulo", "SE": "Sergipe", "TO": "Tocantins"
    }

  def process(self, record):
    """
      Retorna os registros com suas respectivas UFs inseridas
    """
    estado = record[1]
    for key, value in self.ufs.items():
      if estado == value:
        record.insert(0, key)
        yield record # reagrupa o resultado como lista e retorna

EstadosIBGE = (
  estados
  | "Importar Dados" >> beam.io.ReadFromText(r"src\resources\initial\EstadosIBGE.csv", skip_header_lines=1)
  | "Separar dados por ';'" >> beam.Map(lambda record: record.split(';'))
  | "Filtrar estados" >> beam.Map(lambda record: [record[0], record[3]]) # mapeia o resultado para [UF, governador]
  | "Mapeando regiões" >> beam.ParDo(parse_region())
  | "Mapeando UF" >> beam.ParDo(parse_uf())
  | "Filtrando resultado" >> beam.ParDo(filtro_estados_csv()) # formata o resultado
  | "Saida" >> beam.io.WriteToText(file_path_prefix=r"src\resources\processed\processed",
                                            file_name_suffix='.txt', 
                                            shard_name_template='')
)

estados.run()