# -*- coding: utf-8 -*-
import apache_beam as beam

json_parse = beam.Pipeline()

class parse_json(beam.DoFn):

    def process(self, record):
        """
            Retorna o seguinte JSON:
            {
                "Regiao": str,
                "Estado": str,
                "UF": str,
                "Governador": str,
                "TotalCasos": int,
                "TotalObitos": int
            }
        """
        regiao = record[0]
        estado = record[1]
        uf = record[2]
        governador = record[3]
        total_casos = int(record[4])
        total_obitos = int(record[5])
        yield [{
            "Regiao": regiao,
            "Estado": estado,
            "UF": uf,
            "Governador": governador,
            "TotalCasos": total_casos,
            "TotalObitos": total_obitos
        }]

Resultado = (
    json_parse
    | "Lendo dados processados" >>beam.io.ReadFromText(r"src\resources\final\result.csv", skip_header_lines=1)
    | "Separar por ;" >> beam.Map(lambda record: record.split(';'))
    | "Mapeando estados" >> beam.ParDo(parse_json()) # formata para salvar JSON
    | "Saida" >> beam.io.WriteToText(file_path_prefix=r"src\resources\final\result",
                                            file_name_suffix='.json',
                                            shard_name_template='')
)

json_parse.run()